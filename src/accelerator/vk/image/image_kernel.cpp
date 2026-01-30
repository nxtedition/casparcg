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

        CASPAR_LOG(info) << L"[vk::image_kernel] Vulkan rendering kernel initialized (GPU blend modes - Phase 4)";
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

        // Apply transforms to coordinates
        auto& transform = params.transform;

        // Apply fill scale and translation
        for (auto& c : coords) {
            c.vertex_x = c.vertex_x * transform.fill_scale[0] + transform.fill_translation[0];
            c.vertex_y = c.vertex_y * transform.fill_scale[1] + transform.fill_translation[1];
        }

        // Skip if completely outside screen
        if (is_outside_screen(coords)) {
            return;
        }

        // Phase 4: GPU-based compositing with blend modes
        auto src_tex = params.textures[0];
        auto dst_tex = params.background;

        // Only support BGRA format with GPU pipeline for now
        // Other formats still use CPU path (will be addressed in Phase 7)
        if (params.pix_desc.format == core::pixel_format::bgra && src_tex->stride() == 4) {
            // Set up push constants for the compute shader
            blend_push_constants push_constants{};
            push_constants.blend_mode    = static_cast<int32_t>(params.blend_mode);
            push_constants.keyer         = static_cast<int32_t>(params.keyer);
            push_constants.opacity       = static_cast<float>(transform.opacity);
            push_constants.fill_scale_x  = static_cast<float>(transform.fill_scale[0]);
            push_constants.fill_scale_y  = static_cast<float>(transform.fill_scale[1]);
            push_constants.fill_trans_x  = static_cast<float>(transform.fill_translation[0]);
            push_constants.fill_trans_y  = static_cast<float>(transform.fill_translation[1]);
            push_constants.src_width     = src_tex->width();
            push_constants.src_height    = src_tex->height();
            push_constants.dst_width     = dst_tex->width();
            push_constants.dst_height    = dst_tex->height();

            // Execute GPU blend
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
