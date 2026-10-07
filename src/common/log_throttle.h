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
 */

#pragma once

#include <functional>
#include <utility>

namespace caspar {

// Throttles repeated logging of a recurring condition. Call tick() on every
// occurrence and emit a log entry only when it returns true; call reset()
// when the underlying condition clears. The first `initial_delay` occurrences
// are suppressed, then one log is allowed every `period` occurrences, until
// `max_logs` entries have been emitted — after which the throttle stays
// silent until reset.
//
// If `on_silence` is supplied, it is invoked exactly once on the first tick()
// following the final permitted log, so a "silencing" notice naturally lands
// after the caller's last warning in the log. If reset() runs before that
// next tick, the silencing notice is suppressed.
class log_throttle
{
    int                   counter_         = 0;
    int                   logs_emitted_    = 0;
    bool                  silence_pending_ = false;
    int                   initial_delay_;
    int                   period_;
    int                   max_logs_;
    std::function<void()> on_silence_;

  public:
    log_throttle(int initial_delay, int period, int max_logs, std::function<void()> on_silence = {})
        : initial_delay_(initial_delay)
        , period_(period)
        , max_logs_(max_logs)
        , on_silence_(std::move(on_silence))
    {
    }

    bool tick()
    {
        if (silence_pending_) {
            silence_pending_ = false;
            if (on_silence_)
                on_silence_();
        }
        const int prev = counter_++;
        if (logs_emitted_ >= max_logs_)
            return false;
        if (prev < initial_delay_)
            return false;
        if ((counter_ - initial_delay_) % period_ != 0)
            return false;
        if (++logs_emitted_ == max_logs_)
            silence_pending_ = true;
        return true;
    }

    int count() const { return counter_; }

    void reset()
    {
        counter_         = 0;
        logs_emitted_    = 0;
        silence_pending_ = false;
    }
};

} // namespace caspar
