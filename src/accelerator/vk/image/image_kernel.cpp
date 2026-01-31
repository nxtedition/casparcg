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
 */

#include "../StdAfx.h"

#include "image_kernel.h"

#include "../util/buffer.h"
#include "../util/device.h"
#include "../util/matrix.h"
#include "../util/pipeline.h"
#include "../util/texture.h"
#include "../util/vk_check.h"

#include <common/assert.h>
#include <common/bit_depth.h>
#include <common/log.h>

#include <core/frame/frame_transform.h>
#include <core/frame/pixel_format.h>

#include <vulkan/vulkan.h>

#include <array>
#include <cmath>
#include <vector>

namespace caspar { namespace accelerator { namespace vk {

static const double epsilon = 0.001;

// Phase 7: Precision factor based on bit depth (from OGL implementation)
double get_precision_factor(common::bit_depth depth)
{
    switch (depth) {
        case common::bit_depth::bit8:
            return 1.0;
        case common::bit_depth::bit10:
            return 64.0;
        case common::bit_depth::bit12:
            return 16.0;
        case common::bit_depth::bit16:
            return 1.0;
        default:
            return 1.0;
    }
}

// Phase 7: Color conversion matrices for YCbCr to RGB (from OGL implementation)
// Row-major format: [Y_coeff, Cb_coeff, Cr_coeff] for R, G, B
static const float color_matrices[3][9] = {
    {1.0f, 0.0f, 1.402f, 1.0f, -0.344f, -0.509f, 1.0f, 1.772f, 0.0f},                              // BT.601
    {1.0f, 0.0f, 1.5748f, 1.0f, -0.1873f, -0.4681f, 1.0f, 1.8556f, 0.0f},                          // BT.709
    {1.0f, 0.0f, 1.4746f, 1.0f, -0.16455312684366f, -0.57135312684366f, 1.0f, 1.8814f, 0.0f}       // BT.2020
};

// Phase 7: Luma coefficients for color processing
static const float luma_coefficients[3][3] = {
    {0.299f, 0.587f, 0.114f},     // BT.601
    {0.2126f, 0.7152f, 0.0722f},  // BT.709
    {0.2627f, 0.6780f, 0.0593f}   // BT.2020
};

bool is_outside_screen(const std::vector<core::frame_geometry::coord>& coords)
{
    if (coords.empty())
        return true;

    bool all_left = true, all_right = true, all_above = true, all_below = true;

    for (const auto& c : coords) {
        if (c.vertex_x >= 0.0)
            all_left = false;
        if (c.vertex_x <= 1.0)
            all_right = false;
        if (c.vertex_y >= 0.0)
            all_above = false;
        if (c.vertex_y <= 1.0)
            all_below = false;
    }

    return all_left || all_right || all_above || all_below;
}

struct image_kernel::impl
{
    spl::shared_ptr<device> device_;

    // Vulkan handles
    VkDevice         vk_device_       = VK_NULL_HANDLE;
    VkPhysicalDevice physical_device_ = VK_NULL_HANDLE;
    VkCommandPool    command_pool_    = VK_NULL_HANDLE;
    VkQueue          queue_           = VK_NULL_HANDLE;

    // Phase 4: GPU blend pipeline
    std::unique_ptr<blend_pipeline> blend_pipeline_;

    explicit impl(const spl::shared_ptr<device>& dev)
        : device_(dev)
    {
        auto handles     = device_->get_handles();
        vk_device_       = static_cast<VkDevice>(handles.device);
        physical_device_ = static_cast<VkPhysicalDevice>(handles.physical_device);
        command_pool_    = static_cast<VkCommandPool>(handles.command_pool);
        queue_           = static_cast<VkQueue>(handles.queue);

        // Initialize GPU blend pipeline
        blend_pipeline_ = std::make_unique<blend_pipeline>(
            handles.device, handles.physical_device, handles.command_pool, handles.queue);

        CASPAR_LOG(info) << L"[vk::image_kernel] Vulkan rendering kernel initialized (Phase 7 - all pixel formats)";
    }

    ~impl() {}

    void draw(draw_params params)
    {
        if (params.textures.empty() || !params.background) {
            return;
        }

        if (params.transform.opacity < epsilon) {
            return;
        }

        auto coords = params.geometry.data();
        if (coords.empty()) {
            return;
        }

        auto& transform = params.transform;
        auto dst_tex = params.background;

        // Calculate aspect ratio from destination texture
        double aspect_ratio = static_cast<double>(dst_tex->width()) / static_cast<double>(dst_tex->height());

        // Compute the transformation matrix
        mat3 transform_matrix = get_vertex_matrix(transform, aspect_ratio);

        // Phase 7: Set up push constants for all pixel formats
        blend_push_constants push_constants{};

        // Blend parameters
        push_constants.blend_mode = static_cast<int32_t>(params.blend_mode);
        push_constants.keyer      = static_cast<int32_t>(params.keyer);
        push_constants.opacity    = static_cast<float>(transform.opacity);
        push_constants._pad0      = 0;

        // Copy transform matrix
        for (int i = 0; i < 9; ++i) {
            push_constants.transform_matrix[i] = transform_matrix.m[i];
        }
        push_constants._pad1[0] = 0;
        push_constants._pad1[1] = 0;
        push_constants._pad1[2] = 0;

        // Perspective corners
        push_constants.perspective_ul[0] = static_cast<float>(transform.perspective.ul[0]);
        push_constants.perspective_ul[1] = static_cast<float>(transform.perspective.ul[1]);
        push_constants.perspective_ur[0] = static_cast<float>(transform.perspective.ur[0]);
        push_constants.perspective_ur[1] = static_cast<float>(transform.perspective.ur[1]);
        push_constants.perspective_ll[0] = static_cast<float>(transform.perspective.ll[0]);
        push_constants.perspective_ll[1] = static_cast<float>(transform.perspective.ll[1]);
        push_constants.perspective_lr[0] = static_cast<float>(transform.perspective.lr[0]);
        push_constants.perspective_lr[1] = static_cast<float>(transform.perspective.lr[1]);

        // Clipping rectangle
        push_constants.clip_left   = static_cast<float>(transform.clip_translation[0]);
        push_constants.clip_top    = static_cast<float>(transform.clip_translation[1]);
        push_constants.clip_right  = static_cast<float>(transform.clip_translation[0] + transform.clip_scale[0]);
        push_constants.clip_bottom = static_cast<float>(transform.clip_translation[1] + transform.clip_scale[1]);

        // Cropping rectangle
        push_constants.crop_left   = static_cast<float>(transform.crop.ul[0]);
        push_constants.crop_top    = static_cast<float>(transform.crop.ul[1]);
        push_constants.crop_right  = static_cast<float>(transform.crop.lr[0]);
        push_constants.crop_bottom = static_cast<float>(transform.crop.lr[1]);

        // Image dimensions (use first texture for source dimensions)
        auto src_tex = params.textures[0];
        push_constants.src_width  = src_tex->width();
        push_constants.src_height = src_tex->height();
        push_constants.dst_width  = dst_tex->width();
        push_constants.dst_height = dst_tex->height();

        // Feature flags
        push_constants.use_perspective = is_default_perspective(transform.perspective) ? 0 : 1;
        push_constants.use_clipping    = transform.enable_geometry_modifiers ? 1 : 0;
        push_constants.use_cropping    = transform.enable_geometry_modifiers ? 1 : 0;
        push_constants.invert          = transform.invert ? 1 : 0;

        // Phase 6: Color adjustments (Contrast/Saturation/Brightness)
        bool use_csb = std::abs(transform.brightness - 1.0) > epsilon ||
                       std::abs(transform.saturation - 1.0) > epsilon ||
                       std::abs(transform.contrast - 1.0) > epsilon;
        push_constants.use_csb     = use_csb ? 1 : 0;
        push_constants.brightness  = static_cast<float>(transform.brightness);
        push_constants.saturation  = static_cast<float>(transform.saturation);
        push_constants.contrast    = static_cast<float>(transform.contrast);

        // Phase 6: Levels control
        bool use_levels = std::abs(transform.levels.min_input) > epsilon ||
                          std::abs(transform.levels.max_input - 1.0) > epsilon ||
                          std::abs(transform.levels.gamma - 1.0) > epsilon ||
                          std::abs(transform.levels.min_output) > epsilon ||
                          std::abs(transform.levels.max_output - 1.0) > epsilon;
        push_constants.use_levels        = use_levels ? 1 : 0;
        push_constants.levels_min_input  = static_cast<float>(transform.levels.min_input);
        push_constants.levels_max_input  = static_cast<float>(transform.levels.max_input);
        push_constants.levels_gamma      = static_cast<float>(transform.levels.gamma);
        push_constants.levels_min_output = static_cast<float>(transform.levels.min_output);
        push_constants.levels_max_output = static_cast<float>(transform.levels.max_output);
        push_constants._pad2             = 0;
        push_constants._pad3             = 0;

        // Phase 6: Chroma key parameters
        push_constants.use_chroma                       = transform.chroma.enable ? 1 : 0;
        push_constants.chroma_show_mask                 = transform.chroma.show_mask ? 1 : 0;
        push_constants.chroma_target_hue                = static_cast<float>(transform.chroma.target_hue / 360.0);
        push_constants.chroma_hue_width                 = static_cast<float>(transform.chroma.hue_width);
        push_constants.chroma_min_saturation            = static_cast<float>(transform.chroma.min_saturation);
        push_constants.chroma_min_brightness            = static_cast<float>(transform.chroma.min_brightness);
        push_constants.chroma_softness                  = static_cast<float>(transform.chroma.softness);
        push_constants.chroma_spill_suppress            = static_cast<float>(transform.chroma.spill_suppress / 360.0);
        push_constants.chroma_spill_suppress_saturation = static_cast<float>(transform.chroma.spill_suppress_saturation);
        push_constants._pad4[0]                         = 0;
        push_constants._pad4[1]                         = 0;
        push_constants._pad4[2]                         = 0;

        // Phase 7: Pixel format and color space
        push_constants.pixel_format      = static_cast<int32_t>(params.pix_desc.format);
        push_constants.num_planes        = static_cast<int32_t>(params.textures.size());
        push_constants.is_straight_alpha = params.pix_desc.is_straight_alpha ? 1 : 0;

        // Determine color space based on resolution (HD = BT.709, SD = BT.601)
        // If color space is explicitly set, use that; otherwise auto-detect
        const bool is_hd = params.pix_desc.planes.size() > 0 && params.pix_desc.planes[0].height > 700;
        core::color_space effective_color_space = is_hd ? params.pix_desc.color_space : core::color_space::bt601;
        push_constants.color_space = static_cast<int32_t>(effective_color_space);

        // Phase 7: Precision factors for each plane (based on bit depth)
        for (int i = 0; i < 4; ++i) {
            if (i < static_cast<int>(params.textures.size()) && params.textures[i]) {
                push_constants.precision_factor[i] = static_cast<float>(get_precision_factor(params.textures[i]->depth()));
            } else {
                push_constants.precision_factor[i] = 1.0f;
            }
        }

        // Phase 7: Color matrix for YCbCr conversion
        const float* cm = color_matrices[static_cast<int>(effective_color_space)];
        for (int i = 0; i < 9; ++i) {
            push_constants.color_matrix[i] = cm[i];
        }
        // Padding for mat3 alignment in GLSL
        push_constants.color_matrix[9]  = 0.0f;
        push_constants.color_matrix[10] = 0.0f;
        push_constants.color_matrix[11] = 0.0f;

        // Phase 7: Luma coefficients
        const float* lc = luma_coefficients[static_cast<int>(effective_color_space)];
        push_constants.luma_coeff[0] = lc[0];
        push_constants.luma_coeff[1] = lc[1];
        push_constants.luma_coeff[2] = lc[2];
        push_constants.luma_coeff[3] = 0.0f;  // Padding

        // Phase 7: Chroma plane dimensions (for subsampling - YUV420, YUV422)
        if (params.pix_desc.planes.size() > 1) {
            push_constants.plane1_width  = params.pix_desc.planes[1].width;
            push_constants.plane1_height = params.pix_desc.planes[1].height;
        } else {
            push_constants.plane1_width  = src_tex->width();
            push_constants.plane1_height = src_tex->height();
        }
        push_constants._pad5[0] = 0;
        push_constants._pad5[1] = 0;

        // Debug: Log key parameters
        static int debug_kernel_count = 0;
        static int debug_kernel_ok = 0;
        if (debug_kernel_count++ < 30 || debug_kernel_ok < 10) {
            CASPAR_LOG(info) << L"[vk::image_kernel] Blend: opacity=" << push_constants.opacity
                              << L" src=" << push_constants.src_width << L"x" << push_constants.src_height
                              << L" dst=" << push_constants.dst_width << L"x" << push_constants.dst_height
                              << L" pixel_format=" << push_constants.pixel_format
                              << L" transform=[" << push_constants.transform_matrix[0]
                              << L"," << push_constants.transform_matrix[4]
                              << L"," << push_constants.transform_matrix[8] << L"]"
                              << L" fill_scale=[" << transform.fill_scale[0] << L"," << transform.fill_scale[1] << L"]"
                              << L" fill_trans=[" << transform.fill_translation[0] << L"," << transform.fill_translation[1] << L"]"
                              << L" textures=" << params.textures.size()
                              << L" tex0=" << (params.textures.size() > 0 ? (void*)params.textures[0].get() : nullptr);
            if (push_constants.src_width > 1 || push_constants.src_height > 1) {
                debug_kernel_ok++;
            }
        }

        // Execute GPU blend with multi-plane support
        blend_pipeline_->execute(params.textures, *dst_tex, push_constants);
    }
};

image_kernel::image_kernel(const spl::shared_ptr<device>& device)
    : impl_(std::make_unique<impl>(device))
{
}

image_kernel::~image_kernel() {}

void image_kernel::draw(const draw_params& params) { impl_->draw(params); }

}}} // namespace caspar::accelerator::vk
