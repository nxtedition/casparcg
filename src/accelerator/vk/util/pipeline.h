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
 */
struct blend_push_constants
{
    int32_t blend_mode;     // 0-28 blend mode index
    int32_t keyer;          // 0=linear, 1=additive
    float   opacity;        // 0.0-1.0
    float   fill_scale_x;
    float   fill_scale_y;
    float   fill_trans_x;
    float   fill_trans_y;
    int32_t src_width;
    int32_t src_height;
    int32_t dst_width;
    int32_t dst_height;
};

/**
 * Vulkan compute pipeline for blend operations.
 * Phase 4: GPU-accelerated blend modes.
 *
 * Provides:
 * - Compute shader execution for image blending
 * - Support for all 29 Photoshop-compatible blend modes
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
