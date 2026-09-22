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

#include <core/video_format.h>

#pragma warning(push)
#pragma warning(disable : 4458)
#include <include/cef_browser.h>
#pragma warning(pop)

#include <chrono>
#include <memory>
#include <string>

namespace caspar { namespace html {

/**
 * Renders a page on its virtual clock, a frame per pull: each tick advances the page's time by
 * one frame and composites it with external BeginFrames, so animations land on the same frames
 * every run. A paint counts as that frame only if it carries its marker -- the top-left pixel
 * set to a colour unique to the frame, painted over before use -- since a BeginFrame can deliver
 * an older or partly composited one (CEF #4166). tick() runs on the channel thread, the rest on
 * the CEF UI thread.
 */
class virtual_time_driver
{
  public:
    virtual_time_driver(const core::video_format_desc& format_desc, std::wstring name);
    ~virtual_time_driver();

    virtual_time_driver(const virtual_time_driver&)            = delete;
    virtual_time_driver& operator=(const virtual_time_driver&) = delete;

    void attach(CefRefPtr<CefBrowser> browser);

    /** The page is closing or failed to load: stop waiting for frames that will not come. */
    void abort();

    /** Freeze the page clock. From here each tick owns the timeline. */
    void pause_clock();

    /**
     * Advance a frame and wait for its paint until `deadline`. False means not yet: the frame
     * stays in flight, so the next call waits on the same one rather than advancing again.
     */
    bool tick(std::chrono::steady_clock::time_point deadline);

    /** Emulation.virtualTimeBudgetExpired: the clock has reached the frame in flight. */
    void on_budget_expired();

    /** Whether this paint is the frame in flight, and so should be used. */
    bool accept_paint(const void* buffer, int width, int height);

    /** The accepted paint has been queued; the frame is done. */
    void frame_queued();

    /** Paint over the marker, so it never reaches the output. */
    void erase_marker(char* image, int width, int height);

  private:
    struct impl;
    std::unique_ptr<impl> impl_;
};

}} // namespace caspar::html
