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
#include <condition_variable>
#include <mutex>
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

    // A realtime channel always has a reason to tick: with no consumers attached it still
    // runs the loop, pacing itself on the empty-frame shortcut in output.
    bool wait_for_demand() override { return true; }
    void consumers_changed(size_t /*consumer_count*/) override {}
    void abort() override {}
};

class deterministic_pacing final : public channel_pacing
{
    std::mutex              mutex_;
    std::condition_variable cv_;
    size_t                  consumer_count_ = 0;
    bool                    aborted_        = false;

  public:
    // Only produce while something is there to take the frames: without a consumer,
    // free-running would burn through the render with nothing to show for it.
    bool wait_for_demand() override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return aborted_ || consumer_count_ > 0; });
        return !aborted_;
    }

    void consumers_changed(size_t consumer_count) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            consumer_count_ = consumer_count;
        }
        cv_.notify_all();
    }

    void abort() override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            aborted_ = true;
        }
        cv_.notify_all();
    }

    // No wall clock. The channel is throttled by how fast its consumers accept frames,
    // not by how much time has passed.
    void tick(const video_format_desc& /*format_desc*/) override {}
    void reset() override {}
};

} // namespace

spl::shared_ptr<channel_pacing> create_realtime_pacing() { return spl::make_shared<channel_pacing, realtime_pacing>(); }

spl::shared_ptr<channel_pacing> create_deterministic_pacing()
{
    return spl::make_shared<channel_pacing, deterministic_pacing>();
}

}} // namespace caspar::core
