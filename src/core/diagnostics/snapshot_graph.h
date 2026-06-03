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

#include <string>

namespace caspar { namespace core { namespace diagnostics { namespace snapshot {

// Registers a diagnostics sink that continuously records graph data from server startup.
// Coexists with the OSD sink (the diagnostics graph fans out to every registered sink).
//
// enabled            - when false, no sink is registered and no background sampling occurs, so
//                      the feature incurs no overhead (take_snapshot then always fails).
// retention_seconds  - length of recorded history to keep per graph line, in seconds. Values <= 0
//                      fall back to the built-in default window.
void register_sink(bool enabled, int retention_seconds);

// Renders the currently recorded diagnostics history to a PNG image at file_path, mimicking
// the on-screen-display graph (all graphs stacked vertically). Returns true on success.
bool take_snapshot(const std::wstring& file_path);

void shutdown();

}}}} // namespace caspar::core::diagnostics::snapshot
