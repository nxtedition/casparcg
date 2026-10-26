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
 * Author: Robert Nagy, ronag89@gmail.com
 */

#pragma once

#include <common/memory.h>

#include <core/fwd.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace caspar { namespace ffmpeg {

struct resolved_path
{
    std::wstring path;
    bool         cache;
};

// Resolves a producer name (e.g. "shared:https://host/clip.mov" or "clip") to the path ffmpeg opens.
// Empty if the name is not something the ffmpeg producer plays.
std::optional<resolved_path> resolve_path(const std::wstring& name);

// PLAY's SEEK, IN, LENGTH and OUT, in AV_TIME_BASE units.
struct play_range
{
    std::optional<int64_t> start;
    std::optional<int64_t> seek;
    std::optional<int64_t> duration;
};

// Each value is frames of format_desc, or a time (see parse_time). Values in frames throw a user_error when
// format_desc is null.
play_range parse_play_range(const std::vector<std::wstring>& params, const core::video_format_desc* format_desc);

spl::shared_ptr<core::frame_producer> create_producer(const core::frame_producer_dependencies& dependencies,
                                                      const std::vector<std::wstring>&         params);

}} // namespace caspar::ffmpeg
