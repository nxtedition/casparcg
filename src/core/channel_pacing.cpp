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

#include <common/log.h>

#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

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

    // Always a reason to tick: with no consumers the loop still runs, paced by the empty-frame
    // shortcut in output. It never finishes, and shuts down through the channel's abort flag.
    demand wait_for_demand() override { return demand::produce; }
    void   consumers_changed(size_t /*consumer_count*/) override {}
    void   abort() override {}

    // A realtime channel samples its producers and drops a frame if one is not ready;
    // blocking the tick loop on a slow producer would be worse than the dropped frame.
    bool waits_for_producers() const override { return false; }
    bool keep_waiting() const override { return false; }

    void begin_frame() override {}
};

class deterministic_pacing final
    : public channel_pacing
    , public deterministic_controller
{
    mutable std::mutex      mutex_;
    std::condition_variable cv_;
    size_t                  consumer_count_ = 0;
    bool                    aborted_        = false;

    // A render has produced frames and still has a consumer.
    bool running_ = false;

    // A render lost its last consumer and the channel has not reset yet. Recorded when the loss
    // is reported, so a consumer attached again before the channel looks cannot hide it.
    bool finish_pending_ = false;

    // The channel was told the render finished and is resetting itself; it is done once the
    // channel thread comes back to wait_for_demand().
    bool resetting_ = false;

    // Actions by the render frame they run before. A multimap keeps equal keys in insertion
    // order, which is the order actions for the same frame run in.
    schedule schedule_;

    // The render frame the next begin_frame() starts.
    uint64_t next_frame_ = 0;

  public:
    // Only produce while something is there to take the frames: without a consumer,
    // free-running would burn through the render with nothing to show for it.
    demand wait_for_demand() override
    {
        std::unique_lock<std::mutex> lock(mutex_);

        if (finish_pending_ && !aborted_) {
            finish_pending_ = false;
            resetting_      = true;

            // Whatever the render left unrun belongs to it, not to the next one.
            schedule_.clear();
            next_frame_ = 0;

            return demand::finished;
        }

        // Back after being told the render finished, so the channel has reset itself.
        resetting_ = false;

        cv_.wait(lock, [this] { return aborted_ || consumer_count_ > 0; });
        if (aborted_)
            return demand::shutdown;

        running_ = true;
        return demand::produce;
    }

    void consumers_changed(size_t consumer_count) override
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            consumer_count_ = consumer_count;
            if (consumer_count == 0 && running_) {
                running_        = false;
                finish_pending_ = true;
            }
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

    bool waits_for_producers() const override { return true; }

    void begin_frame() override
    {
        std::vector<std::function<void()>> due;
        {
            std::lock_guard<std::mutex> lock(mutex_);

            const auto end = schedule_.upper_bound(next_frame_);
            for (auto it = schedule_.begin(); it != end; ++it)
                due.push_back(std::move(it->second));
            schedule_.erase(schedule_.begin(), end);

            ++next_frame_;
        }

        // Outside the lock: an action can run arbitrary work that calls back into this strategy.
        for (auto& action : due) {
            try {
                action();
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
            }
        }
    }

    bool set_schedule(schedule actions) override
    {
        std::lock_guard<std::mutex> lock(mutex_);

        const bool idle = consumer_count_ == 0 && !finish_pending_ && !resetting_;
        if (!idle)
            return false;

        schedule_ = std::move(actions);
        return true;
    }

    bool keep_waiting() const override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return !aborted_ && consumer_count_ > 0;
    }
};

} // namespace

spl::shared_ptr<channel_pacing> create_realtime_pacing() { return spl::make_shared<channel_pacing, realtime_pacing>(); }

spl::shared_ptr<channel_pacing> create_deterministic_pacing()
{
    return spl::make_shared<channel_pacing, deterministic_pacing>();
}

}} // namespace caspar::core
