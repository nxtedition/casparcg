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

#include "image_mixer.h"
#include "image_kernel.h"

#include "../util/device.h"
#include "../util/texture.h"
#include "../util/vk_check.h"

#include <common/array.h>
#include <common/future.h>
#include <common/log.h>

#include <core/frame/frame.h>
#include <core/frame/frame_transform.h>
#include <core/frame/geometry.h>
#include <core/frame/pixel_format.h>
#include <core/video_format.h>

#include <boost/align/aligned_allocator.hpp>

#include <any>
#include <atomic>
#include <cstring>
#include <stack>
#include <vector>

namespace caspar { namespace accelerator { namespace vk {

using future_texture = std::shared_future<std::shared_ptr<texture>>;

struct item
{
    core::pixel_format_desc     pix_desc = core::pixel_format_desc(core::pixel_format::invalid);
    std::vector<future_texture> textures;
    core::image_transform       transform;
    core::frame_geometry        geometry = core::frame_geometry::get_default();
};

struct layer
{
    std::vector<layer> sublayers;
    std::vector<item>  items;
    core::blend_mode   blend_mode;

    explicit layer(core::blend_mode blend_mode)
        : blend_mode(blend_mode)
    {
    }
};

class image_renderer
{
    spl::shared_ptr<device> device_;
    image_kernel            kernel_;
    const size_t            max_frame_size_;
    common::bit_depth       depth_;

  public:
    explicit image_renderer(const spl::shared_ptr<device>& device, const size_t max_frame_size, common::bit_depth depth)
        : device_(device)
        , kernel_(device_)
        , max_frame_size_(max_frame_size)
        , depth_(depth)
    {
    }

    std::future<array<const std::uint8_t>> operator()(std::vector<layer>             layers,
                                                      const core::video_format_desc& format_desc)
    {
        // Return black frame if device is lost
        if (device_->is_device_lost()) {
            // Periodically attempt recovery (every ~50 frames at 50fps = ~1 second)
            static std::atomic<int> recovery_counter{0};
            if (++recovery_counter % 50 == 0) {
                device_->attempt_recovery();
            }

            static const std::vector<uint8_t, boost::alignment::aligned_allocator<uint8_t, 32>> buffer(
                max_frame_size_, 0);
            return make_ready_future(array<const std::uint8_t>(buffer.data(), format_desc.size, true));
        }

        if (layers.empty()) {
            // Bypass GPU with empty frame (black)
            static const std::vector<uint8_t, boost::alignment::aligned_allocator<uint8_t, 32>> buffer(
                max_frame_size_, 0);
            return make_ready_future(array<const std::uint8_t>(buffer.data(), format_desc.size, true));
        }

        return flatten(device_->dispatch_async(
            [this, format_desc, layers = std::move(layers)]() mutable -> std::shared_future<array<const std::uint8_t>> {
                try {
                    auto target_texture = device_->create_texture(format_desc.width, format_desc.height, 4, depth_);

                    // Clear to transparent black before rendering to avoid undefined content
                    target_texture->clear();

                    draw(target_texture, std::move(layers), format_desc);

                    return device_->copy_async(target_texture);
                } catch (const caspar::vk::device_lost_exception&) {
                    // Mark device as lost and return black frame
                    device_->mark_device_lost();

                    static const std::vector<uint8_t, boost::alignment::aligned_allocator<uint8_t, 32>> buffer(
                        max_frame_size_, 0);
                    return make_ready_future(array<const std::uint8_t>(buffer.data(), format_desc.size, true));
                }
            }));
    }

    common::bit_depth depth() const { return depth_; }

  private:
    void draw(std::shared_ptr<texture>&      target_texture,
              std::vector<layer>             layers,
              const core::video_format_desc& format_desc)
    {
        std::shared_ptr<texture> layer_key_texture;

        for (auto& layer : layers) {
            draw(target_texture, layer.sublayers, format_desc);
            draw(target_texture, std::move(layer), layer_key_texture, format_desc);
        }
    }

    void draw(std::shared_ptr<texture>&      target_texture,
              layer                          layer,
              std::shared_ptr<texture>&      layer_key_texture,
              const core::video_format_desc& format_desc)
    {
        if (layer.items.empty())
            return;

        std::shared_ptr<texture> local_key_texture;
        std::shared_ptr<texture> local_mix_texture;

        if (layer.blend_mode != core::blend_mode::normal) {
            // Non-normal blend modes need precomposition
            auto layer_texture = device_->create_texture(target_texture->width(), target_texture->height(), 4, depth_);
            layer_texture->clear();  // Clear to transparent before compositing

            for (auto& item : layer.items)
                draw(layer_texture, std::move(item), layer_key_texture, local_key_texture, local_mix_texture,
                     format_desc);

            draw(layer_texture, std::move(local_mix_texture), format_desc, core::blend_mode::normal);
            draw(target_texture, std::move(layer_texture), format_desc, layer.blend_mode);
        } else {
            // Fast path for normal blend mode
            for (auto& item : layer.items)
                draw(target_texture, std::move(item), layer_key_texture, local_key_texture, local_mix_texture,
                     format_desc);

            draw(target_texture, std::move(local_mix_texture), format_desc, core::blend_mode::normal);
        }

        layer_key_texture = std::move(local_key_texture);
    }

    void draw(std::shared_ptr<texture>&      target_texture,
              item                           item,
              std::shared_ptr<texture>&      layer_key_texture,
              std::shared_ptr<texture>&      local_key_texture,
              std::shared_ptr<texture>&      local_mix_texture,
              const core::video_format_desc& format_desc)
    {
        draw_params params;
        params.target_width  = format_desc.square_width;
        params.target_height = format_desc.square_height;
        params.pix_desc      = std::move(item.pix_desc);
        params.transform     = std::move(item.transform);
        params.geometry      = std::move(item.geometry);
        params.aspect_ratio =
            static_cast<double>(format_desc.square_width) / static_cast<double>(format_desc.square_height);

        for (auto& future_texture : item.textures) {
            params.textures.push_back(future_texture.get());
        }

        if (params.transform.is_key) {
            // Key: use as mask for next non-key item
            if (!local_key_texture) {
                local_key_texture = device_->create_texture(target_texture->width(), target_texture->height(), 1, depth_);
                local_key_texture->clear();
            }

            params.background = local_key_texture;
            params.local_key  = nullptr;
            params.layer_key  = nullptr;

            kernel_.draw(params);
        } else if (params.transform.is_mix) {
            // Mix: precompose items before drawing to channel
            if (!local_mix_texture) {
                local_mix_texture = device_->create_texture(target_texture->width(), target_texture->height(), 4, depth_);
                local_mix_texture->clear();
            }

            params.background = local_mix_texture;
            params.local_key  = std::move(local_key_texture);
            params.layer_key  = layer_key_texture;
            params.keyer      = keyer::additive;

            kernel_.draw(params);
        } else {
            // Normal: draw directly to target
            draw(target_texture, std::move(local_mix_texture), format_desc, core::blend_mode::normal);

            params.background = target_texture;
            params.local_key  = std::move(local_key_texture);
            params.layer_key  = layer_key_texture;

            kernel_.draw(params);
        }
    }

    void draw(std::shared_ptr<texture>&  target_texture,
              std::shared_ptr<texture>&& source_texture,
              core::video_format_desc    format_desc,
              core::blend_mode           blend_mode = core::blend_mode::normal)
    {
        if (!source_texture)
            return;

        draw_params params;
        params.target_width    = format_desc.square_width;
        params.target_height   = format_desc.square_height;
        params.pix_desc.format = core::pixel_format::bgra;
        params.pix_desc.planes = {core::pixel_format_desc::plane(
            source_texture->width(), source_texture->height(), 4, source_texture->depth())};
        params.textures   = {source_texture};
        params.blend_mode = blend_mode;
        params.background = target_texture;
        params.geometry   = core::frame_geometry::get_default();

        kernel_.draw(params);
    }
};

struct image_mixer::impl
    : public core::frame_factory
    , public std::enable_shared_from_this<impl>
{
    spl::shared_ptr<device>               device_;
    image_renderer                        renderer_;
    std::vector<core::image_transform>    transform_stack_;
    std::vector<layer>                    layers_;
    std::vector<layer*>                   layer_stack_;
    int                                   channel_id_;
    double                                aspect_ratio_ = 1.0;

  public:
    impl(const spl::shared_ptr<device>& device,
         int                            channel_id,
         const size_t                   max_frame_size,
         common::bit_depth              depth)
        : device_(device)
        , renderer_(device_, max_frame_size, depth)
        , transform_stack_(1)
        , channel_id_(channel_id)
    {
        CASPAR_LOG(info) << L"[vk::image_mixer] Vulkan Image Mixer initialized for channel " << channel_id_;
    }

    ~impl()
    {
        // CRITICAL: Wait for all pending Vulkan operations to complete before destruction.
        // The image_renderer captures 'this' in lambdas dispatched to the Vulkan thread.
        // If we destroy without synchronizing, those lambdas will execute with a dangling pointer.
        try {
            CASPAR_LOG(debug) << L"[vk::image_mixer] Channel " << channel_id_ << L" - waiting for pending GPU operations...";
            device_->dispatch_sync([] {
                // Empty lambda - just wait for all prior dispatched work to complete
            });
            CASPAR_LOG(debug) << L"[vk::image_mixer] Channel " << channel_id_ << L" - GPU sync complete, destroying.";
        } catch (...) {
            CASPAR_LOG(warning) << L"[vk::image_mixer] Channel " << channel_id_ << L" - exception during GPU sync on destruction.";
        }
    }

    void update_aspect_ratio(double aspect_ratio) { aspect_ratio_ = aspect_ratio; }

    void push(const core::frame_transform& transform)
    {
        auto previous_layer_depth = transform_stack_.back().layer_depth;

        // Combine transforms
        auto combined = transform_stack_.back();
        combined.opacity *= transform.image_transform.opacity;
        combined.brightness *= transform.image_transform.brightness;
        combined.contrast *= transform.image_transform.contrast;
        combined.saturation *= transform.image_transform.saturation;

        // Combine fill transforms
        combined.fill_translation[0] =
            combined.fill_translation[0] + transform.image_transform.fill_translation[0] * combined.fill_scale[0];
        combined.fill_translation[1] =
            combined.fill_translation[1] + transform.image_transform.fill_translation[1] * combined.fill_scale[1];
        combined.fill_scale[0] *= transform.image_transform.fill_scale[0];
        combined.fill_scale[1] *= transform.image_transform.fill_scale[1];

        // Copy other properties
        combined.blend_mode  = transform.image_transform.blend_mode;
        combined.layer_depth = transform.image_transform.layer_depth;
        // is_key and is_mix should be OR'd, not replaced - if any parent has is_key=true, keep it
        combined.is_key      = combined.is_key || transform.image_transform.is_key;
        combined.is_mix      = combined.is_mix || transform.image_transform.is_mix;
        combined.invert      = transform.image_transform.invert;
        combined.levels      = transform.image_transform.levels;
        combined.chroma      = transform.image_transform.chroma;

        transform_stack_.push_back(combined);

        auto new_layer_depth = transform_stack_.back().layer_depth;

        if (previous_layer_depth < new_layer_depth) {
            layer new_layer(transform_stack_.back().blend_mode);

            if (layer_stack_.empty()) {
                layers_.push_back(std::move(new_layer));
                layer_stack_.push_back(&layers_.back());
            } else {
                layer_stack_.back()->sublayers.push_back(std::move(new_layer));
                layer_stack_.push_back(&layer_stack_.back()->sublayers.back());
            }
        }
    }

    void visit(const core::const_frame& frame)
    {
        if (frame.pixel_format_desc().format == core::pixel_format::invalid)
            return;

        if (frame.pixel_format_desc().planes.empty())
            return;

        // Safety check: ensure we have a layer context
        if (layer_stack_.empty()) {
            CASPAR_LOG(warning) << L"[vk::image_mixer] visit() called without layer context, creating default layer";
            layers_.push_back(layer(core::blend_mode::normal));
            layer_stack_.push_back(&layers_.back());
        }

        // Safety check: ensure transform stack is not empty
        if (transform_stack_.empty()) {
            CASPAR_LOG(warning) << L"[vk::image_mixer] visit() called without transform context";
            return;
        }

        item item;
        item.pix_desc  = frame.pixel_format_desc();
        item.transform = transform_stack_.back();
        item.geometry  = frame.geometry();

        // Check if frame already has GPU textures cached
        auto textures_ptr = std::any_cast<std::shared_ptr<std::vector<future_texture>>>(frame.opaque());

        if (textures_ptr) {
            item.textures = *textures_ptr;
        } else {
            // Upload frame data to GPU
            for (int n = 0; n < static_cast<int>(item.pix_desc.planes.size()); ++n) {
                auto future = device_->copy_async(frame.image_data(n),
                                                  item.pix_desc.planes[n].width,
                                                  item.pix_desc.planes[n].height,
                                                  item.pix_desc.planes[n].stride,
                                                  item.pix_desc.planes[n].depth);
                item.textures.emplace_back(future.share());
            }
        }

        layer_stack_.back()->items.push_back(item);
    }

    void pop()
    {
        transform_stack_.pop_back();
        layer_stack_.resize(transform_stack_.back().layer_depth);
    }

    std::future<array<const std::uint8_t>> render(const core::video_format_desc& format_desc)
    {
        return renderer_(std::move(layers_), format_desc);
    }

    core::mutable_frame create_frame(const void* tag, const core::pixel_format_desc& desc) override
    {
        return create_frame(tag, desc, common::bit_depth::bit8);
    }

    core::mutable_frame
    create_frame(const void* tag, const core::pixel_format_desc& desc, common::bit_depth depth) override
    {
        std::vector<array<std::uint8_t>> image_data;
        for (auto& plane : desc.planes) {
            auto bytes_per_pixel = depth == common::bit_depth::bit8 ? 1 : 2;
            image_data.push_back(device_->create_array(plane.size * bytes_per_pixel));
        }

        std::weak_ptr<image_mixer::impl> weak_self = shared_from_this();
        return core::mutable_frame(tag,
                                   std::move(image_data),
                                   array<int32_t>{},
                                   desc,
                                   [weak_self, desc](std::vector<array<const std::uint8_t>> image_data) -> std::any {
                                       auto self = weak_self.lock();
                                       if (!self) {
                                           return std::any{};
                                       }
                                       std::vector<future_texture> textures;
                                       for (int n = 0; n < static_cast<int>(desc.planes.size()); ++n) {
                                           auto future = self->device_->copy_async(image_data[n],
                                                                                   desc.planes[n].width,
                                                                                   desc.planes[n].height,
                                                                                   desc.planes[n].stride,
                                                                                   desc.planes[n].depth);
                                           textures.emplace_back(future.share());
                                       }
                                       return std::make_shared<decltype(textures)>(std::move(textures));
                                   });
    }

    common::bit_depth depth() const { return renderer_.depth(); }
};

image_mixer::image_mixer(const spl::shared_ptr<device>& device,
                         int                            channel_id,
                         const size_t                   max_frame_size,
                         common::bit_depth              depth)
    : impl_(std::make_shared<impl>(device, channel_id, max_frame_size, depth))
{
}

image_mixer::~image_mixer() {}

std::future<array<const std::uint8_t>> image_mixer::render(const core::video_format_desc& format_desc)
{
    return impl_->render(format_desc);
}

core::mutable_frame image_mixer::create_frame(const void* tag, const core::pixel_format_desc& desc)
{
    return impl_->create_frame(tag, desc);
}

core::mutable_frame
image_mixer::create_frame(const void* video_stream_tag, const core::pixel_format_desc& desc, common::bit_depth depth)
{
    return impl_->create_frame(video_stream_tag, desc, depth);
}

void image_mixer::update_aspect_ratio(double aspect_ratio) { impl_->update_aspect_ratio(aspect_ratio); }

void image_mixer::push(const core::frame_transform& transform) { impl_->push(transform); }

void image_mixer::visit(const core::const_frame& frame) { impl_->visit(frame); }

void image_mixer::pop() { impl_->pop(); }

common::bit_depth image_mixer::depth() const { return impl_->depth(); }

}}} // namespace caspar::accelerator::vk
