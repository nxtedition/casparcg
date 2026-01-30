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

        CASPAR_LOG(info) << L"[vk::image_kernel] Vulkan rendering kernel initialized (GPU color processing - Phase 6)";
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

        // Phase 5: GPU-based compositing with full geometric transforms
        auto src_tex = params.textures[0];
        auto dst_tex = params.background;

        // Calculate aspect ratio from destination texture
        double aspect_ratio = static_cast<double>(dst_tex->width()) / static_cast<double>(dst_tex->height());

        // Compute the transformation matrix
        mat3 transform_matrix = get_vertex_matrix(transform, aspect_ratio);

        // Only support BGRA format with GPU pipeline for now
        // Other formats still use CPU path (will be addressed in Phase 7)
        if (params.pix_desc.format == core::pixel_format::bgra && src_tex->stride() == 4) {
            // Set up push constants for the compute shader
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

            // Image dimensions
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

            // Execute GPU blend with transforms
            blend_pipeline_->execute(*src_tex, *dst_tex, push_constants);
        } else {
            // Fallback to CPU compositing for non-BGRA formats
            draw_cpu_fallback(params);
        }
    }

    // CPU fallback for non-BGRA formats (will be replaced in Phase 7)
    void draw_cpu_fallback(draw_params& params)
    {
        auto src_tex = params.textures[0];
        auto dst_tex = params.background;
        auto& transform = params.transform;

        auto src_size = src_tex->size();
        auto dst_size = dst_tex->size();

        // Allocate staging buffers
        auto src_buffer = std::make_shared<buffer>(vk_device_, physical_device_, src_size, false);
        auto dst_buffer = std::make_shared<buffer>(vk_device_, physical_device_, dst_size, true);

        // Read textures
        src_tex->copy_to(*src_buffer);
        dst_tex->copy_to(*dst_buffer);

        auto src_data = reinterpret_cast<const uint8_t*>(src_buffer->data());
        auto dst_data = reinterpret_cast<uint8_t*>(dst_buffer->data());

        int src_width  = src_tex->width();
        int src_height = src_tex->height();
        int src_stride = src_tex->stride();
        int dst_width  = dst_tex->width();
        int dst_height = dst_tex->height();

        int dst_x = static_cast<int>(transform.fill_translation[0] * dst_width);
        int dst_y = static_cast<int>(transform.fill_translation[1] * dst_height);
        int dst_w = static_cast<int>(transform.fill_scale[0] * dst_width);
        int dst_h = static_cast<int>(transform.fill_scale[1] * dst_height);

        dst_x = std::max(0, std::min(dst_x, dst_width - 1));
        dst_y = std::max(0, std::min(dst_y, dst_height - 1));
        dst_w = std::max(1, std::min(dst_w, dst_width - dst_x));
        dst_h = std::max(1, std::min(dst_h, dst_height - dst_y));

        float opacity = static_cast<float>(transform.opacity);

        if (params.pix_desc.format == core::pixel_format::gray && src_stride == 1) {
            // Grayscale to BGRA - for key textures
            for (int y = 0; y < dst_h && y < src_height; ++y) {
                for (int x = 0; x < dst_w && x < src_width; ++x) {
                    int src_idx = y * src_width + x;
                    int dst_idx = ((dst_y + y) * dst_width + (dst_x + x)) * 4;

                    if (dst_idx + 3 >= dst_size || src_idx >= src_size)
                        continue;

                    float gray = src_data[src_idx] / 255.0f * opacity;

                    dst_data[dst_idx + 0] = static_cast<uint8_t>(gray * 255.0f);
                    dst_data[dst_idx + 1] = static_cast<uint8_t>(gray * 255.0f);
                    dst_data[dst_idx + 2] = static_cast<uint8_t>(gray * 255.0f);
                    dst_data[dst_idx + 3] = 255;
                }
            }
        }
        // TODO: Handle other pixel formats (YCbCr, etc.) in Phase 7

        dst_tex->copy_from(*dst_buffer);
    }
};

image_kernel::image_kernel(const spl::shared_ptr<device>& device)
    : impl_(std::make_unique<impl>(device))
{
}

image_kernel::~image_kernel() {}

void image_kernel::draw(const draw_params& params) { impl_->draw(params); }

}}} // namespace caspar::accelerator::vk
