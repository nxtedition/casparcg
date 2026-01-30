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

// Stub implementation of osd_graph for non-OpenGL builds (macOS with Vulkan)
// OSD diagnostics rendering is disabled in this build

#include "../StdAfx.h"

#include "osd_graph.h"

#include <common/diagnostics/graph.h>

namespace caspar { namespace core { namespace diagnostics { namespace osd {

void register_sink()
{
    // No-op: OSD diagnostics not available in Vulkan build
}

void show_graphs(bool)
{
    // No-op: OSD diagnostics not available in Vulkan build
}

void shutdown()
{
    // No-op: OSD diagnostics not available in Vulkan build
}

}}}} // namespace caspar::core::diagnostics::osd
