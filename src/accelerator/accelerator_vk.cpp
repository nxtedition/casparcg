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

// Vulkan-based accelerator implementation for macOS

#include "accelerator.h"

#include "vk/image/image_mixer.h"
#include "vk/util/device.h"

#include <boost/property_tree/ptree.hpp>

#include <common/bit_depth.h>

#include <core/mixer/image/image_mixer.h>

#include <memory>
#include <mutex>
#include <utility>

namespace caspar { namespace accelerator {

struct accelerator::impl
{
    std::shared_ptr<vk::device>         vk_device_;
    const core::video_format_repository format_repository_;

    impl(const core::video_format_repository format_repository)
        : format_repository_(format_repository)
    {
    }

    std::unique_ptr<core::image_mixer> create_image_mixer(int channel_id, common::bit_depth depth)
    {
        return std::make_unique<vk::image_mixer>(
            spl::make_shared_ptr(get_device()), channel_id, format_repository_.get_max_video_format_size(), depth);
    }

    std::shared_ptr<vk::device> get_device()
    {
        if (!vk_device_) {
            vk_device_ = std::make_shared<vk::device>();
        }

        return vk_device_;
    }
};

accelerator::accelerator(const core::video_format_repository format_repository)
    : impl_(std::make_unique<impl>(format_repository))
{
}

accelerator::~accelerator() {}

std::unique_ptr<core::image_mixer> accelerator::create_image_mixer(const int channel_id, common::bit_depth depth)
{
    return impl_->create_image_mixer(channel_id, depth);
}

std::shared_ptr<accelerator_device> accelerator::get_device() const
{
    return std::dynamic_pointer_cast<accelerator_device>(impl_->get_device());
}

}} // namespace caspar::accelerator
