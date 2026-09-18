/*
 * Copyright (c) 2026 Sveriges Television AB <info@casparcg.com>
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
 */

#include "StdAfx.h"

#include "channel_pacing.h"

#include <chrono>
#include <optional>
#include <thread>

namespace caspar { namespace core {

namespace {

using time_point_t = std::chrono::high_resolution_clock::time_point;

class realtime_pacing final : public channel_pacing
{
    // The instant the next frame is due. Absent until the first tick(), and dropped
    // whenever reset() decides the schedule should restart from now.
    std::optional<time_point_t> deadline_;

  public:
    void tick(const video_format_desc& format_desc) override
    {
        if (!deadline_) {
            // Nothing armed: start the schedule here rather than sleeping.
            deadline_ = std::chrono::high_resolution_clock::now();
        } else {
            std::this_thread::sleep_until(*deadline_);
        }

        *deadline_ += std::chrono::microseconds(static_cast<int>(1e6 / format_desc.hz));
    }

    void reset() override { deadline_.reset(); }
};

} // namespace

spl::shared_ptr<channel_pacing> create_realtime_pacing() { return spl::make_shared<channel_pacing, realtime_pacing>(); }

}} // namespace caspar::core
