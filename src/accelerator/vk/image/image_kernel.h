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

#include <common/memory.h>

#include <core/frame/frame_transform.h>
#include <core/frame/geometry.h>
#include <core/frame/pixel_format.h>
#include <core/mixer/image/blend_modes.h>

#include <memory>
#include <vector>

namespace caspar { namespace accelerator { namespace vk {

class texture;
class device;

enum class keyer
{
    linear = 0,
    additive,
};

/**
 * Draw parameters for Vulkan rendering
 * Matches the OGL pattern for compatibility
 */
struct draw_params final
{
    core::pixel_format_desc                pix_desc = core::pixel_format_desc(core::pixel_format::invalid);
    std::vector<std::shared_ptr<texture>>  textures;
    core::image_transform                  transform;
    core::frame_geometry                   geometry   = core::frame_geometry::get_default();
    core::blend_mode                       blend_mode = core::blend_mode::normal;
    keyer                                  keyer      = keyer::linear;
    std::shared_ptr<texture>               background;
    std::shared_ptr<texture>               local_key;
    std::shared_ptr<texture>               layer_key;
    double                                 aspect_ratio = 1.0;
    int                                    target_width;
    int                                    target_height;
};

/**
 * Vulkan image kernel - Phase 3 implementation
 *
 * Handles low-level GPU rendering operations:
 * - Quad rendering for texture compositing
 * - Pipeline setup and uniform binding
 * - Texture sampling and alpha blending
 * - Basic blend modes (normal/over mode for Phase 3)
 */
class image_kernel final
{
  public:
    explicit image_kernel(const spl::shared_ptr<device>& device);
    ~image_kernel();

    image_kernel(const image_kernel&)            = delete;
    image_kernel& operator=(const image_kernel&) = delete;

    void draw(const draw_params& params);

  private:
    struct impl;
    std::unique_ptr<impl> impl_;
};

}}} // namespace caspar::accelerator::vk
