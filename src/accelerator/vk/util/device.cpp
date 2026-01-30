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

#include "device.h"

#include <common/log.h>

#include <boost/property_tree/ptree.hpp>

namespace caspar { namespace accelerator { namespace vk {

struct device::impl
{
    impl()
    {
        CASPAR_LOG(info) << L"Vulkan device stub initialized (rendering disabled)";
    }

    ~impl()
    {
        CASPAR_LOG(info) << L"Vulkan device stub destroyed";
    }

    array<uint8_t> create_array(int size)
    {
        // Create a CPU-side array (no GPU allocation in stub)
        return array<uint8_t>(static_cast<size_t>(size));
    }

    std::wstring version() const
    {
        return L"Vulkan Stub 1.0 (No GPU Rendering)";
    }

    boost::property_tree::wptree info() const
    {
        boost::property_tree::wptree info;
        info.put(L"name", L"Vulkan Stub Device");
        info.put(L"version", version());
        info.put(L"status", L"stub - rendering disabled");
        return info;
    }

    std::future<void> gc()
    {
        return std::async(std::launch::deferred, []() {
            // No-op in stub
        });
    }
};

device::device()
    : impl_(std::make_shared<impl>())
{
}

device::~device() {}

array<uint8_t> device::create_array(int size)
{
    return impl_->create_array(size);
}

std::wstring device::version() const
{
    return impl_->version();
}

boost::property_tree::wptree device::info() const
{
    return impl_->info();
}

std::future<void> device::gc()
{
    return impl_->gc();
}

}}} // namespace caspar::accelerator::vk
