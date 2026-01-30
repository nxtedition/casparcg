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

#include "../util/device.h"

#include <common/array.h>
#include <common/log.h>

#include <core/frame/frame.h>
#include <core/frame/frame_transform.h>
#include <core/frame/pixel_format.h>
#include <core/video_format.h>

#include <cstring>
#include <stack>

namespace caspar { namespace accelerator { namespace vk {

struct image_mixer::impl
{
    spl::shared_ptr<device> device_;
    int                     channel_id_;
    size_t                  max_frame_size_;
    common::bit_depth       depth_;
    double                  aspect_ratio_ = 1.0;

    std::stack<core::frame_transform> transform_stack_;

    impl(const spl::shared_ptr<device>& device,
         int                            channel_id,
         const size_t                   max_frame_size,
         common::bit_depth              depth)
        : device_(device)
        , channel_id_(channel_id)
        , max_frame_size_(max_frame_size)
        , depth_(depth)
    {
        transform_stack_.push(core::frame_transform());
        CASPAR_LOG(info) << L"[vk::image_mixer] Stub initialized for channel " << channel_id_
                         << L" (rendering disabled)";
    }

    std::future<array<const std::uint8_t>> render(const core::video_format_desc& format_desc)
    {
        // Return a black frame
        auto frame_size = format_desc.size;
        auto result     = array<std::uint8_t>(frame_size);

        // Fill with black (BGRA format: B=0, G=0, R=0, A=255)
        for (size_t i = 0; i < frame_size; i += 4) {
            result.data()[i + 0] = 0;   // B
            result.data()[i + 1] = 0;   // G
            result.data()[i + 2] = 0;   // R
            result.data()[i + 3] = 255; // A
        }

        return std::async(std::launch::deferred, [r = std::move(result)]() mutable {
            return array<const std::uint8_t>(std::move(r));
        });
    }

    core::mutable_frame create_frame(const void* tag, const core::pixel_format_desc& desc, common::bit_depth depth)
    {
        std::vector<array<std::uint8_t>> image_data;
        for (auto& plane : desc.planes) {
            // Create CPU-side array
            image_data.push_back(array<std::uint8_t>(static_cast<size_t>(plane.size)));
        }

        return core::mutable_frame(tag, std::move(image_data), array<int32_t>{}, desc);
    }

    void update_aspect_ratio(double aspect_ratio) { aspect_ratio_ = aspect_ratio; }

    void push(const core::frame_transform& transform)
    {
        // In the stub, just push the transform directly (no composition needed)
        transform_stack_.push(transform);
    }

    void visit(const core::const_frame& frame)
    {
        // No-op in stub - frames are accepted but not rendered
    }

    void pop()
    {
        if (transform_stack_.size() > 1) {
            transform_stack_.pop();
        }
    }
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
    return impl_->create_frame(tag, desc, impl_->depth_);
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

common::bit_depth image_mixer::depth() const { return impl_->depth_; }

}}} // namespace caspar::accelerator::vk
