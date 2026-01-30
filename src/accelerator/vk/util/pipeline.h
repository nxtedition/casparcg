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

#pragma once

#include <core/mixer/image/blend_modes.h>

#include <cstdint>
#include <memory>

namespace caspar { namespace accelerator { namespace vk {

class texture;

/**
 * Push constants structure for blend compute shader.
 * Must match the layout in blend.comp
 *
 * Phase 5: Extended with full transform matrix and geometry parameters.
 */
struct blend_push_constants
{
    // Blend parameters
    int32_t blend_mode;     // 0-28 blend mode index
    int32_t keyer;          // 0=linear, 1=additive
    float   opacity;        // 0.0-1.0
    int32_t _pad0;          // Padding for alignment

    // Transform matrix (3x3, column-major, stored as 3 vec3s for GLSL compatibility)
    // Row 0: m[0], m[1], m[2]
    // Row 1: m[3], m[4], m[5]
    // Row 2: m[6], m[7], m[8]
    float transform_matrix[9];
    float _pad1[3];         // Padding to align next field

    // Perspective corners (for bilinear interpolation)
    // Each corner is (x, y): ul, ur, ll, lr
    float perspective_ul[2];
    float perspective_ur[2];
    float perspective_ll[2];
    float perspective_lr[2];

    // Clipping rectangle (normalized 0-1)
    float clip_left;
    float clip_top;
    float clip_right;
    float clip_bottom;

    // Cropping rectangle (normalized 0-1)
    float crop_left;
    float crop_top;
    float crop_right;
    float crop_bottom;

    // Image dimensions
    int32_t src_width;
    int32_t src_height;
    int32_t dst_width;
    int32_t dst_height;

    // Feature flags
    int32_t use_perspective;    // 1 if perspective is non-default
    int32_t use_clipping;       // 1 if clipping should be applied
    int32_t use_cropping;       // 1 if cropping should be applied
    int32_t _pad2;              // Padding for alignment
};

/**
 * Vulkan compute pipeline for blend operations.
 * Phase 4: GPU-accelerated blend modes.
 * Phase 5: Geometric transforms (FILL, ROTATION, PERSPECTIVE, CLIP, CROP).
 *
 * Provides:
 * - Compute shader execution for image blending
 * - Support for all 29 Photoshop-compatible blend modes
 * - Full geometric transforms with matrix math
 * - Perspective distortion via bilinear corner interpolation
 * - Clipping and cropping support
 * - Push constant based parameter passing
 */
class blend_pipeline final
{
  public:
    // device, physical_device, command_pool, queue are VkHandle cast to void*
    blend_pipeline(void* device, void* physical_device, void* command_pool, void* queue);
    ~blend_pipeline();

    blend_pipeline(const blend_pipeline&)            = delete;
    blend_pipeline& operator=(const blend_pipeline&) = delete;

    /**
     * Execute blend operation using compute shader.
     *
     * @param src Source texture (read-only)
     * @param dst Destination texture (read-write)
     * @param params Blend parameters
     */
    void execute(texture& src, texture& dst, const blend_push_constants& params);

  private:
    struct impl;
    std::unique_ptr<impl> impl_;
};

}}} // namespace caspar::accelerator::vk
