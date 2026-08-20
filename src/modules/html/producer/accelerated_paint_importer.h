/*
 * Copyright 2025
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
 * Author: Niklas Andersson, niklas@niklaspandersson.se
 */

#pragma once

#include <common/memory.h>

#include <core/frame/frame.h>

#include <memory>
#include <string>

#include <include/cef_render_handler.h>

namespace caspar { namespace core {
class frame_factory;
}} // namespace caspar::core

namespace caspar { namespace html {

// Consumes CEF's OnAcceleratedPaint shared texture without a CPU round trip.
//
// OnPaint hands over a pointer to system memory, which means the browser's frame has already
// been read back off the GPU; the producer then memcpys it and the mixer uploads it straight
// back. OnAcceleratedPaint instead hands over the texture the browser's compositor rendered
// into. This imports that texture and copies it, on the GPU, into a texture the mixer can
// sample — the pixels never leave the device.
//
// Not zero-copy, deliberately: CEF returns the buffer to its pool "after this callback
// returns", so the imported image cannot be handed to the mixer to sample whenever it gets
// round to it. One GPU-local copy per frame buys the right to let go of the buffer, and costs
// a fraction of the readback it replaces.
//
// This type is available on every platform. Whether it can actually do anything is a runtime
// question — ask usable(), and report unusable_reason() when it cannot, because "your GPU
// cannot do this" and "this build cannot do this" look identical from the outside otherwise.
//
// Lives on, and is only ever touched from, the CEF UI thread.
class accelerated_paint_importer final
{
  public:
    explicit accelerated_paint_importer(const spl::shared_ptr<core::frame_factory>& frame_factory);
    ~accelerated_paint_importer();

    accelerated_paint_importer(const accelerated_paint_importer&)            = delete;
    accelerated_paint_importer& operator=(const accelerated_paint_importer&) = delete;

    // Whether to ask CEF for shared textures at all. Decide this once, BEFORE the browser is
    // created: shared_texture_enabled picks OnAcceleratedPaint over OnPaint for the browser's
    // whole life, and there is no per-frame fallback to a path CEF has stopped calling.
    bool usable() const;

    // Why not, when usable() is false. Names the missing capability so the log can say which.
    const std::wstring& unusable_reason() const;

    // Import one shared texture and copy it into a frame of the channel's size. Returns an
    // empty frame if the buffer cannot be consumed, in which case the caller should keep
    // showing the previous one.
    core::const_frame
    import(const void* tag, const CefAcceleratedPaintInfo& info, int dst_width, int dst_height);

  private:
    struct impl;
    std::unique_ptr<impl> impl_;
};

}} // namespace caspar::html
