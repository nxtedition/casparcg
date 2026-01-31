/*
 * Copyright (c) 2011 Sveriges Television AB <info@casparcg.com>
 *
 * This file is part of CasparCG (www.casparcg.com).
 *
 * CasparCG is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CasparCG is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CasparCG. If not, see <http://www.gnu.org/licenses/>.
 *
 * Author: CasparCG Team
 *
 * Vulkan fragment shader for image compositing with blend modes.
 * Graphics pipeline version for MoltenVK compatibility.
 *
 * Phase 15: Graphics-based blend pipeline (replaces compute shader)
 */

#version 450

// Input from vertex shader
layout(location = 0) in vec2 in_tex_coord;

// Output color
layout(location = 0) out vec4 out_color;

// Source texture samplers (up to 4 planes for planar formats)
layout(binding = 0) uniform sampler2D plane0;
layout(binding = 1) uniform sampler2D plane1;
layout(binding = 2) uniform sampler2D plane2;
layout(binding = 3) uniform sampler2D plane3;

// Destination texture sampler (for complex blend modes that need to read dst)
layout(binding = 4) uniform sampler2D dst_sampler;

// Push constants for parameters
// Must match blend_push_constants in pipeline.h EXACTLY (scalar layout)
// Using float arrays instead of mat3/vec3 to match C++ struct layout
layout(push_constant) uniform PushConstants {
    // Blend parameters (16 bytes)
    int   blend_mode;      // 0-28 blend mode index
    int   keyer;           // 0=linear, 1=additive
    float opacity;         // 0.0-1.0
    int   _pad0;

    // Transform matrix (3x3, stored as float[9] + float[3] pad = 48 bytes)
    float transform_matrix[9];
    float _pad1[3];

    // Perspective corners (ul, ur, ll, lr - each is float[2], total 32 bytes)
    float perspective_ul[2];
    float perspective_ur[2];
    float perspective_ll[2];
    float perspective_lr[2];

    // Clipping rectangle (16 bytes)
    float clip_left;
    float clip_top;
    float clip_right;
    float clip_bottom;

    // Cropping rectangle (16 bytes)
    float crop_left;
    float crop_top;
    float crop_right;
    float crop_bottom;

    // Image dimensions (16 bytes)
    int   src_width;
    int   src_height;
    int   dst_width;
    int   dst_height;

    // Feature flags (16 bytes)
    int   use_perspective;
    int   use_clipping;
    int   use_cropping;
    int   invert;

    // Phase 6: Color adjustments (16 bytes)
    int   use_csb;
    float brightness;
    float saturation;
    float contrast;

    // Phase 6: Levels control (16 bytes)
    int   use_levels;
    float levels_min_input;
    float levels_max_input;
    float levels_gamma;

    // Levels output + padding (16 bytes)
    float levels_min_output;
    float levels_max_output;
    int   _pad2;
    int   _pad3;

    // Phase 6: Chroma key parameters (16 bytes)
    int   use_chroma;
    int   chroma_show_mask;
    float chroma_target_hue;
    float chroma_hue_width;

    // Chroma key continued (16 bytes)
    float chroma_min_saturation;
    float chroma_min_brightness;
    float chroma_softness;
    float chroma_spill_suppress;

    // Chroma spill + padding (16 bytes)
    float chroma_spill_suppress_saturation;
    int   _pad4[3];

    // Phase 7: Pixel format and color space (16 bytes)
    int   pixel_format;        // 0-12
    int   color_space;         // 0=bt601, 1=bt709, 2=bt2020
    int   num_planes;          // 1-4
    int   is_straight_alpha;   // 1 if non-premultiplied

    // Precision factors (16 bytes)
    float precision_factor[4];

    // Color matrix (3x3 + 3 padding, stored as float[12] = 48 bytes)
    float color_matrix[12];

    // Luma coefficients (16 bytes)
    float luma_coeff[4];

    // Plane 1 dimensions (16 bytes)
    int   plane1_width;
    int   plane1_height;
    int   _pad5[2];
} params;

// ============================================================================
// Helper Functions for Array-to-Matrix Conversion
// ============================================================================

// Construct mat3 from float[9] array
// Input is ROW-MAJOR from C++ (each row = [Y_coeff, Cb_coeff, Cr_coeff] for R, G, B)
// GLSL mat3 constructor expects COLUMN-MAJOR, so we transpose here
mat3 array_to_mat3(float arr[9])
{
    // Transpose: row-major input to column-major mat3
    return mat3(
        arr[0], arr[3], arr[6],  // Column 0: Y coefficients for R, G, B
        arr[1], arr[4], arr[7],  // Column 1: Cb coefficients for R, G, B
        arr[2], arr[5], arr[8]   // Column 2: Cr coefficients for R, G, B
    );
}

// Construct mat3 from float[12] array (with padding)
// Same transpose logic as array_to_mat3
mat3 array12_to_mat3(float arr[12])
{
    // Transpose: row-major input to column-major mat3
    return mat3(
        arr[0], arr[3], arr[6],  // Column 0: Y coefficients for R, G, B
        arr[1], arr[4], arr[7],  // Column 1: Cb coefficients for R, G, B
        arr[2], arr[5], arr[8]   // Column 2: Cr coefficients for R, G, B
    );
}

// Get vec3 from luma coefficients array
vec3 get_luma_coeff()
{
    return vec3(params.luma_coeff[0], params.luma_coeff[1], params.luma_coeff[2]);
}

// ============================================================================
// Pixel Format Constants
// ============================================================================

#define PIXEL_FORMAT_GRAY    0
#define PIXEL_FORMAT_BGRA    1
#define PIXEL_FORMAT_RGBA    2
#define PIXEL_FORMAT_ARGB    3
#define PIXEL_FORMAT_ABGR    4
#define PIXEL_FORMAT_YCBCR   5
#define PIXEL_FORMAT_YCBCRA  6
#define PIXEL_FORMAT_LUMA    7
#define PIXEL_FORMAT_BGR     8
#define PIXEL_FORMAT_RGB     9
#define PIXEL_FORMAT_UYVY    10
#define PIXEL_FORMAT_GBRP    11
#define PIXEL_FORMAT_GBRAP   12

// ============================================================================
// YCbCr to RGBA Conversion
// ============================================================================

vec4 ycbcra_to_rgba(float Y, float Cb, float Cr, float A)
{
    const float luma_coefficient = 255.0 / 219.0;
    const float chroma_coefficient = 255.0 / 224.0;

    vec3 YCbCr = vec3(Y, Cb, Cr) * 255.0;
    YCbCr -= vec3(16.0, 128.0, 128.0);
    YCbCr *= vec3(luma_coefficient, chroma_coefficient, chroma_coefficient);

    mat3 color_mat = array12_to_mat3(params.color_matrix);
    vec3 rgb = color_mat * YCbCr / 255.0;
    return vec4(rgb, A);  // Output RGBA directly, no swizzle needed
}

// ============================================================================
// Pixel Format Decoding
// ============================================================================

vec4 get_rgba_color(vec2 tex_coord)
{
    switch (params.pixel_format)
    {
    case PIXEL_FORMAT_GRAY:
        {
            vec4 texel = texture(plane0, tex_coord);
            float gray = texel.r * params.precision_factor[0];
            return vec4(gray, gray, gray, 1.0);
        }

    case PIXEL_FORMAT_BGRA:
        {
            vec4 texel = texture(plane0, tex_coord);
            return texel.bgra * params.precision_factor[0];
        }

    case PIXEL_FORMAT_RGBA:
        {
            vec4 texel = texture(plane0, tex_coord);
            return texel * params.precision_factor[0];
        }

    case PIXEL_FORMAT_ARGB:
        {
            vec4 texel = texture(plane0, tex_coord);
            return texel.gbar * params.precision_factor[0];
        }

    case PIXEL_FORMAT_ABGR:
        {
            vec4 texel = texture(plane0, tex_coord);
            return texel.abgr * params.precision_factor[0];
        }

    case PIXEL_FORMAT_YCBCR:
        {
            float Y  = texture(plane0, tex_coord).r * params.precision_factor[0];
            float Cb = texture(plane1, tex_coord).r * params.precision_factor[1];
            float Cr = texture(plane2, tex_coord).r * params.precision_factor[2];
            return ycbcra_to_rgba(Y, Cb, Cr, 1.0);
        }

    case PIXEL_FORMAT_YCBCRA:
        {
            float Y  = texture(plane0, tex_coord).r * params.precision_factor[0];
            float Cb = texture(plane1, tex_coord).r * params.precision_factor[1];
            float Cr = texture(plane2, tex_coord).r * params.precision_factor[2];
            float A  = texture(plane3, tex_coord).r * params.precision_factor[3];
            return ycbcra_to_rgba(Y, Cb, Cr, A);
        }

    case PIXEL_FORMAT_LUMA:
        {
            float luma = texture(plane0, tex_coord).r * params.precision_factor[0];
            return vec4(luma, luma, luma, 1.0);
        }

    case PIXEL_FORMAT_BGR:
        {
            vec4 texel = texture(plane0, tex_coord);
            return vec4(texel.bgr, 1.0) * params.precision_factor[0];
        }

    case PIXEL_FORMAT_RGB:
        {
            vec4 texel = texture(plane0, tex_coord);
            return vec4(texel.rgb, 1.0) * params.precision_factor[0];
        }

    case PIXEL_FORMAT_GBRP:
        {
            // GBRP: Green, Blue, Red in separate planes
            float g = texture(plane0, tex_coord).r * params.precision_factor[0];
            float b = texture(plane1, tex_coord).r * params.precision_factor[1];
            float r = texture(plane2, tex_coord).r * params.precision_factor[2];
            return vec4(r, g, b, 1.0);
        }

    case PIXEL_FORMAT_GBRAP:
        {
            // GBRAP: Green, Blue, Red, Alpha in separate planes
            float g = texture(plane0, tex_coord).r * params.precision_factor[0];
            float b = texture(plane1, tex_coord).r * params.precision_factor[1];
            float r = texture(plane2, tex_coord).r * params.precision_factor[2];
            float a = texture(plane3, tex_coord).r * params.precision_factor[3];
            return vec4(r, g, b, a);
        }

    default:
        return texture(plane0, tex_coord);
    }
}

// ============================================================================
// HSL Color Space Conversion
// ============================================================================

vec3 RGBToHSL(vec3 color)
{
    vec3 hsl;
    float fmin = min(min(color.r, color.g), color.b);
    float fmax = max(max(color.r, color.g), color.b);
    float delta = fmax - fmin;

    hsl.z = (fmax + fmin) / 2.0;

    if (delta == 0.0) {
        hsl.x = 0.0;
        hsl.y = 0.0;
    } else {
        if (hsl.z < 0.5)
            hsl.y = delta / (fmax + fmin);
        else
            hsl.y = delta / (2.0 - fmax - fmin);

        float deltaR = (((fmax - color.r) / 6.0) + (delta / 2.0)) / delta;
        float deltaG = (((fmax - color.g) / 6.0) + (delta / 2.0)) / delta;
        float deltaB = (((fmax - color.b) / 6.0) + (delta / 2.0)) / delta;

        if (color.r == fmax)
            hsl.x = deltaB - deltaG;
        else if (color.g == fmax)
            hsl.x = (1.0 / 3.0) + deltaR - deltaB;
        else if (color.b == fmax)
            hsl.x = (2.0 / 3.0) + deltaG - deltaR;

        if (hsl.x < 0.0)
            hsl.x += 1.0;
        else if (hsl.x > 1.0)
            hsl.x -= 1.0;
    }

    return hsl;
}

float HueToRGB(float f1, float f2, float hue)
{
    if (hue < 0.0) hue += 1.0;
    else if (hue > 1.0) hue -= 1.0;
    float res;
    if ((6.0 * hue) < 1.0)
        res = f1 + (f2 - f1) * 6.0 * hue;
    else if ((2.0 * hue) < 1.0)
        res = f2;
    else if ((3.0 * hue) < 2.0)
        res = f1 + (f2 - f1) * ((2.0 / 3.0) - hue) * 6.0;
    else
        res = f1;
    return res;
}

vec3 HSLToRGB(vec3 hsl)
{
    vec3 rgb;
    if (hsl.y == 0.0) {
        rgb = vec3(hsl.z);
    } else {
        float f2;
        if (hsl.z < 0.5)
            f2 = hsl.z * (1.0 + hsl.y);
        else
            f2 = hsl.z + hsl.y - hsl.y * hsl.z;
        float f1 = 2.0 * hsl.z - f2;
        rgb.r = HueToRGB(f1, f2, hsl.x + (1.0/3.0));
        rgb.g = HueToRGB(f1, f2, hsl.x);
        rgb.b = HueToRGB(f1, f2, hsl.x - (1.0/3.0));
    }
    return rgb;
}

// ============================================================================
// HSV Color Space Conversion
// ============================================================================

vec3 rgb2hsv(vec3 c)
{
    vec4 K = vec4(0.0, -1.0 / 3.0, 2.0 / 3.0, -1.0);
    vec4 p = mix(vec4(c.bg, K.wz), vec4(c.gb, K.xy), step(c.b, c.g));
    vec4 q = mix(vec4(p.xyw, c.r), vec4(c.r, p.yzx), step(p.x, c.r));
    float d = q.x - min(q.w, q.y);
    float e = 1.0e-10;
    return vec3(abs(q.z + (q.w - q.y) / (6.0 * d + e)), d / (q.x + e), q.x);
}

vec3 hsv2rgb(vec3 c)
{
    vec4 K = vec4(1.0, 2.0 / 3.0, 1.0 / 3.0, 3.0);
    vec3 p = abs(fract(c.xxx + K.xyz) * 6.0 - K.www);
    return c.z * mix(K.xxx, clamp(p - K.xxx, 0.0, 1.0), c.y);
}

// ============================================================================
// Color Processing Functions
// ============================================================================

vec3 ContrastSaturationBrightness(vec4 color, float brt, float sat, float con)
{
    const float AvgLumR = 0.5;
    const float AvgLumG = 0.5;
    const float AvgLumB = 0.5;
    vec3 LumCoeff = get_luma_coeff().bgr;

    if (color.a > 0.0)
        color.rgb /= color.a;

    vec3 AvgLumin = vec3(AvgLumR, AvgLumG, AvgLumB);
    vec3 brtColor = color.rgb * brt;
    vec3 intensity = vec3(dot(brtColor, LumCoeff));
    vec3 satColor = mix(intensity, brtColor, sat);
    vec3 conColor = mix(AvgLumin, satColor, con);
    conColor.rgb *= color.a;
    return conColor;
}

vec3 LevelsControl(vec3 color, float minInput, float gamma, float maxInput, float minOutput, float maxOutput)
{
    vec3 result = min(max(color - vec3(minInput), vec3(0.0)) / (vec3(maxInput) - vec3(minInput)), vec3(1.0));
    result = pow(result, vec3(1.0 / gamma));
    result = mix(vec3(minOutput), vec3(maxOutput), result);
    return result;
}

// ============================================================================
// Chroma Key Functions
// ============================================================================

float AngleDiff(float angle1, float angle2)
{
    return 0.5 - abs(abs(angle1 - angle2) - 0.5);
}

float AngleDiffDirectional(float angle1, float angle2)
{
    float diff = angle1 - angle2;
    return diff < -0.5 ? diff + 1.0 : (diff > 0.5 ? diff - 1.0 : diff);
}

float ChromaDistance(float actual, float target)
{
    return min(0.0, target - actual);
}

float ColorDistance(vec3 hsv)
{
    float hueDiff = AngleDiff(hsv.x, params.chroma_target_hue) * 2.0;
    float saturationDiff = ChromaDistance(hsv.y, params.chroma_min_saturation);
    float brightnessDiff = ChromaDistance(hsv.z, params.chroma_min_brightness);
    float saturationBrightnessScore = max(brightnessDiff, saturationDiff);
    float hueScore = hueDiff - params.chroma_hue_width;
    return -hueScore * saturationBrightnessScore;
}

float alpha_map(float d)
{
    return 1.0 - smoothstep(1.0, params.chroma_softness, d);
}

vec3 suppress_spill(vec3 hsv)
{
    float hue = hsv.x;
    float diff = AngleDiffDirectional(hue, params.chroma_target_hue);
    float distance = abs(diff) / params.chroma_spill_suppress;

    if (distance < 1.0 && params.chroma_spill_suppress > 0.0)
    {
        hsv.x = diff < 0.0
                ? params.chroma_target_hue - params.chroma_spill_suppress
                : params.chroma_target_hue + params.chroma_spill_suppress;
        hsv.y *= min(1.0, distance + params.chroma_spill_suppress_saturation);
    }
    return hsv;
}

vec4 ChromaOnCustomColor(vec4 c)
{
    vec3 hsv = rgb2hsv(c.rgb);
    float distance = ColorDistance(hsv);
    float d = distance * -2.0 + 1.0;
    vec4 suppressed = vec4(hsv2rgb(suppress_spill(hsv)), 1.0);
    float alpha = alpha_map(d);
    suppressed *= alpha;

    return params.chroma_show_mask != 0
           ? vec4(suppressed.a, suppressed.a, suppressed.a, 1.0)
           : suppressed;
}

// ============================================================================
// Blend Mode Functions
// ============================================================================

float BlendAddf(float base, float blend) { return min(base + blend, 1.0); }
float BlendSubstractf(float base, float blend) { return max(base + blend - 1.0, 0.0); }
float BlendLightenf(float base, float blend) { return max(blend, base); }
float BlendDarkenf(float base, float blend) { return min(blend, base); }
float BlendScreenf(float base, float blend) { return 1.0 - ((1.0 - base) * (1.0 - blend)); }
float BlendOverlayf(float base, float blend) { return base < 0.5 ? (2.0 * base * blend) : (1.0 - 2.0 * (1.0 - base) * (1.0 - blend)); }
float BlendSoftLightf(float base, float blend) { return (blend < 0.5) ? (2.0 * base * blend + base * base * (1.0 - 2.0 * blend)) : (sqrt(base) * (2.0 * blend - 1.0) + 2.0 * base * (1.0 - blend)); }
float BlendColorDodgef(float base, float blend) { return (blend == 1.0) ? blend : min(base / (1.0 - blend), 1.0); }
float BlendColorBurnf(float base, float blend) { return (blend == 0.0) ? blend : max((1.0 - ((1.0 - base) / blend)), 0.0); }
float BlendLinearDodgef(float base, float blend) { return BlendAddf(base, blend); }
float BlendLinearBurnf(float base, float blend) { return BlendSubstractf(base, blend); }
float BlendLinearLightf(float base, float blend) { return blend < 0.5 ? BlendLinearBurnf(base, (2.0 * blend)) : BlendLinearDodgef(base, (2.0 * (blend - 0.5))); }
float BlendVividLightf(float base, float blend) { return (blend < 0.5) ? BlendColorBurnf(base, (2.0 * blend)) : BlendColorDodgef(base, (2.0 * (blend - 0.5))); }
float BlendPinLightf(float base, float blend) { return (blend < 0.5) ? BlendDarkenf(base, (2.0 * blend)) : BlendLightenf(base, (2.0 * (blend - 0.5))); }
float BlendHardMixf(float base, float blend) { return (BlendVividLightf(base, blend) < 0.5) ? 0.0 : 1.0; }
float BlendReflectf(float base, float blend) { return (blend == 1.0) ? blend : min(base * base / (1.0 - blend), 1.0); }

vec3 BlendNormal(vec3 base, vec3 blend) { return blend; }
vec3 BlendLighten(vec3 base, vec3 blend) { return vec3(BlendLightenf(base.r, blend.r), BlendLightenf(base.g, blend.g), BlendLightenf(base.b, blend.b)); }
vec3 BlendDarken(vec3 base, vec3 blend) { return vec3(BlendDarkenf(base.r, blend.r), BlendDarkenf(base.g, blend.g), BlendDarkenf(base.b, blend.b)); }
vec3 BlendMultiply(vec3 base, vec3 blend) { return base * blend; }
vec3 BlendAverage(vec3 base, vec3 blend) { return (base + blend) / 2.0; }
vec3 BlendAdd(vec3 base, vec3 blend) { return min(base + blend, vec3(1.0)); }
vec3 BlendSubstract(vec3 base, vec3 blend) { return max(base + blend - vec3(1.0), vec3(0.0)); }
vec3 BlendDifference(vec3 base, vec3 blend) { return abs(base - blend); }
vec3 BlendNegation(vec3 base, vec3 blend) { return vec3(1.0) - abs(vec3(1.0) - base - blend); }
vec3 BlendExclusion(vec3 base, vec3 blend) { return base + blend - 2.0 * base * blend; }
vec3 BlendScreen(vec3 base, vec3 blend) { return vec3(BlendScreenf(base.r, blend.r), BlendScreenf(base.g, blend.g), BlendScreenf(base.b, blend.b)); }
vec3 BlendOverlay(vec3 base, vec3 blend) { return vec3(BlendOverlayf(base.r, blend.r), BlendOverlayf(base.g, blend.g), BlendOverlayf(base.b, blend.b)); }
vec3 BlendSoftLight(vec3 base, vec3 blend) { return vec3(BlendSoftLightf(base.r, blend.r), BlendSoftLightf(base.g, blend.g), BlendSoftLightf(base.b, blend.b)); }
vec3 BlendHardLight(vec3 base, vec3 blend) { return BlendOverlay(blend, base); }
vec3 BlendColorDodge(vec3 base, vec3 blend) { return vec3(BlendColorDodgef(base.r, blend.r), BlendColorDodgef(base.g, blend.g), BlendColorDodgef(base.b, blend.b)); }
vec3 BlendColorBurn(vec3 base, vec3 blend) { return vec3(BlendColorBurnf(base.r, blend.r), BlendColorBurnf(base.g, blend.g), BlendColorBurnf(base.b, blend.b)); }
vec3 BlendLinearDodge(vec3 base, vec3 blend) { return BlendAdd(base, blend); }
vec3 BlendLinearBurn(vec3 base, vec3 blend) { return BlendSubstract(base, blend); }
vec3 BlendLinearLight(vec3 base, vec3 blend) { return vec3(BlendLinearLightf(base.r, blend.r), BlendLinearLightf(base.g, blend.g), BlendLinearLightf(base.b, blend.b)); }
vec3 BlendVividLight(vec3 base, vec3 blend) { return vec3(BlendVividLightf(base.r, blend.r), BlendVividLightf(base.g, blend.g), BlendVividLightf(base.b, blend.b)); }
vec3 BlendPinLight(vec3 base, vec3 blend) { return vec3(BlendPinLightf(base.r, blend.r), BlendPinLightf(base.g, blend.g), BlendPinLightf(base.b, blend.b)); }
vec3 BlendHardMix(vec3 base, vec3 blend) { return vec3(BlendHardMixf(base.r, blend.r), BlendHardMixf(base.g, blend.g), BlendHardMixf(base.b, blend.b)); }
vec3 BlendReflect(vec3 base, vec3 blend) { return vec3(BlendReflectf(base.r, blend.r), BlendReflectf(base.g, blend.g), BlendReflectf(base.b, blend.b)); }
vec3 BlendGlow(vec3 base, vec3 blend) { return BlendReflect(blend, base); }
vec3 BlendPhoenix(vec3 base, vec3 blend) { return min(base, blend) - max(base, blend) + vec3(1.0); }

vec3 BlendHue(vec3 base, vec3 blend) {
    vec3 baseHSL = RGBToHSL(base);
    return HSLToRGB(vec3(RGBToHSL(blend).r, baseHSL.g, baseHSL.b));
}

vec3 BlendSaturation(vec3 base, vec3 blend) {
    vec3 baseHSL = RGBToHSL(base);
    return HSLToRGB(vec3(baseHSL.r, RGBToHSL(blend).g, baseHSL.b));
}

vec3 BlendColor(vec3 base, vec3 blend) {
    vec3 blendHSL = RGBToHSL(blend);
    return HSLToRGB(vec3(blendHSL.r, blendHSL.g, RGBToHSL(base).b));
}

vec3 BlendLuminosity(vec3 base, vec3 blend) {
    vec3 baseHSL = RGBToHSL(base);
    return HSLToRGB(vec3(baseHSL.r, baseHSL.g, RGBToHSL(blend).b));
}

vec3 get_blend_color(vec3 back, vec3 fore)
{
    switch(params.blend_mode)
    {
    case  0: return BlendNormal(back, fore);
    case  1: return BlendLighten(back, fore);
    case  2: return BlendDarken(back, fore);
    case  3: return BlendMultiply(back, fore);
    case  4: return BlendAverage(back, fore);
    case  5: return BlendAdd(back, fore);
    case  6: return BlendSubstract(back, fore);
    case  7: return BlendDifference(back, fore);
    case  8: return BlendNegation(back, fore);
    case  9: return BlendExclusion(back, fore);
    case 10: return BlendScreen(back, fore);
    case 11: return BlendOverlay(back, fore);
    case 12: return BlendSoftLight(back, fore);
    case 13: return BlendHardLight(back, fore);
    case 14: return BlendColorDodge(back, fore);
    case 15: return BlendColorBurn(back, fore);
    case 16: return BlendLinearDodge(back, fore);
    case 17: return BlendLinearBurn(back, fore);
    case 18: return BlendLinearLight(back, fore);
    case 19: return BlendVividLight(back, fore);
    case 20: return BlendPinLight(back, fore);
    case 21: return BlendHardMix(back, fore);
    case 22: return BlendReflect(back, fore);
    case 23: return BlendGlow(back, fore);
    case 24: return BlendPhoenix(back, fore);
    case 25: return BlendHue(back, fore);
    case 26: return BlendSaturation(back, fore);
    case 27: return BlendColor(back, fore);
    case 28: return BlendLuminosity(back, fore);
    }
    return BlendNormal(back, fore);
}

// ============================================================================
// Transform Functions
// ============================================================================

mat3 inverse_mat3(mat3 m)
{
    float det = m[0][0] * (m[1][1] * m[2][2] - m[2][1] * m[1][2])
              - m[0][1] * (m[1][0] * m[2][2] - m[1][2] * m[2][0])
              + m[0][2] * (m[1][0] * m[2][1] - m[1][1] * m[2][0]);

    if (abs(det) < 0.0001)
        return mat3(1.0);

    float inv_det = 1.0 / det;
    mat3 result;
    result[0][0] = (m[1][1] * m[2][2] - m[2][1] * m[1][2]) * inv_det;
    result[0][1] = (m[0][2] * m[2][1] - m[0][1] * m[2][2]) * inv_det;
    result[0][2] = (m[0][1] * m[1][2] - m[0][2] * m[1][1]) * inv_det;
    result[1][0] = (m[1][2] * m[2][0] - m[1][0] * m[2][2]) * inv_det;
    result[1][1] = (m[0][0] * m[2][2] - m[0][2] * m[2][0]) * inv_det;
    result[1][2] = (m[1][0] * m[0][2] - m[0][0] * m[1][2]) * inv_det;
    result[2][0] = (m[1][0] * m[2][1] - m[2][0] * m[1][1]) * inv_det;
    result[2][1] = (m[2][0] * m[0][1] - m[0][0] * m[2][1]) * inv_det;
    result[2][2] = (m[0][0] * m[1][1] - m[1][0] * m[0][1]) * inv_det;
    return result;
}

vec2 transform_point(mat3 m, vec2 p)
{
    vec3 result = m * vec3(p, 1.0);
    return result.xy;
}

// ============================================================================
// Main Entry Point
// ============================================================================

void main()
{
    vec2 dst_pos = in_tex_coord;

    // Apply clipping check (in destination space)
    if (params.use_clipping != 0)
    {
        if (dst_pos.x < params.clip_left || dst_pos.x > params.clip_right ||
            dst_pos.y < params.clip_top || dst_pos.y > params.clip_bottom)
        {
            discard;
        }
    }

    // Apply inverse transform to get source coordinates
    mat3 transform_mat = array_to_mat3(params.transform_matrix);
    mat3 inv_transform = inverse_mat3(transform_mat);
    vec2 src_pos = transform_point(inv_transform, dst_pos);

    // Check if source coordinate is in valid range (0-1)
    if (src_pos.x < 0.0 || src_pos.x > 1.0 || src_pos.y < 0.0 || src_pos.y > 1.0)
    {
        discard;
    }

    // Apply cropping check (in source space)
    if (params.use_cropping != 0)
    {
        if (src_pos.x < params.crop_left || src_pos.x > params.crop_right ||
            src_pos.y < params.crop_top || src_pos.y > params.crop_bottom)
        {
            discard;
        }
    }

    // Get source color using pixel format
    vec4 src_color = get_rgba_color(src_pos);

    // Apply opacity
    src_color.a *= params.opacity;

    // Apply chroma key if enabled
    if (params.use_chroma != 0)
    {
        src_color = ChromaOnCustomColor(src_color);
    }

    // Apply color adjustments
    if (params.use_csb != 0)
    {
        src_color.rgb = ContrastSaturationBrightness(src_color, params.brightness, params.saturation, params.contrast);
    }

    // Apply levels
    if (params.use_levels != 0)
    {
        src_color.rgb = LevelsControl(src_color.rgb, params.levels_min_input, params.levels_gamma,
                                       params.levels_max_input, params.levels_min_output, params.levels_max_output);
    }

    // Apply invert
    if (params.invert != 0)
    {
        src_color.rgb = vec3(1.0) - src_color.rgb;
    }

    // Convert to straight alpha if needed (premultiply)
    if (params.is_straight_alpha != 0 && src_color.a > 0.0)
    {
        src_color.rgb *= src_color.a;
    }

    // For blend modes other than normal, we need to read the destination
    if (params.blend_mode != 0)
    {
        vec4 dst_color = texture(dst_sampler, dst_pos);

        // Unpremultiply for blend calculation
        vec3 back_rgb = dst_color.a > 0.0001 ? dst_color.rgb / dst_color.a : vec3(0.0);
        vec3 fore_rgb = src_color.a > 0.0001 ? src_color.rgb / src_color.a : vec3(0.0);

        // Apply blend mode
        vec3 blended = get_blend_color(back_rgb, fore_rgb);

        // Composite with alpha
        switch (params.keyer)
        {
            case 1:  // additive
                out_color = vec4(blended * src_color.a, src_color.a) + dst_color;
                break;
            default: // linear (over)
                out_color = vec4(blended * src_color.a, src_color.a) + (1.0 - src_color.a) * dst_color;
                break;
        }
    }
    else
    {
        // Normal blend mode - output premultiplied color for hardware blending
        out_color = src_color;
    }
}
