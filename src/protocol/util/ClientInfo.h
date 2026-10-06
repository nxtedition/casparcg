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
 * Author: Nicklas P Andersson
 */

#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "protocol_strategy.h"
#include <common/log.h>

namespace caspar { namespace IO {

typedef spl::shared_ptr<client_connection<wchar_t>> ClientInfoPtr;
typedef std::shared_ptr<client_connection<wchar_t>> ClientInfoPtrStd;

struct ConsoleClientInfo : public client_connection<wchar_t>
{
    void send(std::wstring&& data, bool skip_log) override
    {
        std::wcout << L"#" + caspar::log::replace_nonprintable_copy(data, L'?') << std::flush;
    }
    void         disconnect() override {}
    std::wstring address() const override { return L"Console"; }

    // Kept rather than dropped: a caller binds an object here to have it live as long as the
    // client does, and the console client lives as long as the server. Dropping it ends what
    // it was bound to the moment it is handed over.
    void add_lifecycle_bound_object(const std::wstring& key, const std::shared_ptr<void>& lifecycle_bound) override
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        lifecycle_bound_objects_.insert_or_assign(key, lifecycle_bound);
    }

    std::shared_ptr<void> remove_lifecycle_bound_object(const std::wstring& key) override
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);

        auto it = lifecycle_bound_objects_.find(key);
        if (it == lifecycle_bound_objects_.end())
            return std::shared_ptr<void>();

        auto result = it->second;
        lifecycle_bound_objects_.erase(it);
        return result;
    }

  private:
    std::mutex                                   lifecycle_mutex_;
    std::map<std::wstring, std::shared_ptr<void>> lifecycle_bound_objects_;
};

}} // namespace caspar::IO
