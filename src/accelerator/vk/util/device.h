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

#include <accelerator/accelerator.h>
#include <common/array.h>
#include <common/bit_depth.h>

#include <functional>
#include <future>

namespace caspar { namespace accelerator { namespace vk {

/**
 * Vulkan device - stub implementation for Phase 1
 *
 * This is a minimal stub that allows CasparCG to compile and run on macOS
 * without actual GPU rendering. Real Vulkan rendering will be implemented
 * in Phase 2.
 */
class device final
    : public std::enable_shared_from_this<device>
    , public accelerator_device
{
  public:
    device();
    ~device();

    device(const device&) = delete;
    device& operator=(const device&) = delete;

    // Buffer/array creation
    array<uint8_t> create_array(int size);

    // Async dispatch (executes synchronously in stub)
    template <typename Func>
    auto dispatch_async(Func&& func)
    {
        using result_type = decltype(func());
        using task_type   = std::packaged_task<result_type()>;

        auto task   = std::make_shared<task_type>(std::forward<Func>(func));
        auto future = task->get_future();
        (*task)(); // Execute synchronously in stub
        return future;
    }

    template <typename Func>
    auto dispatch_sync(Func&& func)
    {
        return dispatch_async(std::forward<Func>(func)).get();
    }

    std::wstring version() const;

    // accelerator_device interface
    boost::property_tree::wptree info() const override;
    std::future<void>            gc() override;

  private:
    struct impl;
    std::shared_ptr<impl> impl_;
};

}}} // namespace caspar::accelerator::vk
