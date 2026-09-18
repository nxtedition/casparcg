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

#pragma once

#include <common/memory.h>
#include <core/video_format.h>

namespace caspar { namespace core {

/**
 * Decides how fast a channel's loop may run: for a realtime channel the wall clock, unless a
 * consumer brings its own synchronization clock and throttles the loop itself. Owned by the
 * channel and shared with its output, so every decision about its rate lives in one place.
 */
class channel_pacing
{
  public:
    channel_pacing()          = default;
    virtual ~channel_pacing() = default;

    channel_pacing(const channel_pacing&)            = delete;
    channel_pacing& operator=(const channel_pacing&) = delete;

    /**
     * Block until the next frame is due, then arm the following deadline. Once per tick, from
     * the channel thread, after the frame has gone to the consumers.
     */
    virtual void tick(const video_format_desc& format_desc) = 0;

    /**
     * Discard the armed deadline, so the next tick() paces from then: after a format change, a
     * frame that was not emitted, or when a consumer paces the channel itself.
     */
    virtual void reset() = 0;
};

/**
 * Paces the channel against the wall clock at the format's frame rate.
 */
spl::shared_ptr<channel_pacing> create_realtime_pacing();

}} // namespace caspar::core
