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

#include "virtual_time_driver.h"

#include <common/log.h>

#pragma warning(push)
#pragma warning(disable : 4458)
#include <include/cef_values.h>
#pragma warning(pop)

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <optional>

#include "../util.h"

namespace caspar { namespace html {

namespace {
constexpr int SYNC_PUMP_MS    = 4;   // pacing between begin-frames, to let the renderer composite
constexpr int SYNC_TIMEOUT_MS = 250; // after this, take the next paint whatever its marker
} // namespace

struct virtual_time_driver::impl
{
    const core::video_format_desc format_desc_;
    const std::wstring            name_;

    CefRefPtr<CefBrowser> browser_;
    std::atomic<bool>     aborted_{false};

    std::mutex              mutex_;
    std::condition_variable cv_;

    // A frame is in flight from advancing the clock until its paint is accepted. Callers wait
    // in slices, so a later call has to wait on the same frame rather than advance again.
    bool                                                 in_flight_      = false;
    bool                                                 budget_expired_ = false;
    bool                                                 paint_received_ = false;
    bool                                                 force_accept_   = false;
    bool                                                 slow_reported_  = false;
    std::chrono::steady_clock::time_point                started_;
    std::optional<std::chrono::steady_clock::time_point> sync_deadline_;

    uint64_t frame_count_ = 0;

    // Marker state, touched only on the UI thread.
    uint8_t          expected_marker_[3] = {0, 0, 0};
    std::atomic<int> begin_frames_{0}; // diagnostics
    std::atomic<int> paints_{0};       // diagnostics

    impl(const core::video_format_desc& format_desc, std::wstring name)
        : format_desc_(format_desc)
        , name_(std::move(name))
    {
    }

    // Cumulative virtual time at the end of frame `n`, from the rational frame duration so that
    // non-integer rates do not drift.
    uint64_t virtual_time_us_at(uint64_t n) const
    {
        const uint64_t num = static_cast<uint64_t>(format_desc_.duration) * 1'000'000ULL;
        const uint64_t den = static_cast<uint64_t>(format_desc_.time_scale);
        return den > 0 ? (n * num) / den : 0;
    }

    void pause_clock()
    {
        if (!browser_)
            return;

        auto policy = CefDictionaryValue::Create();
        policy->SetString("policy", "pause");
        browser_->GetHost()->ExecuteDevToolsMethod(0, "Emulation.setVirtualTimePolicy", policy);
    }

    bool tick(std::chrono::steady_clock::time_point deadline)
    {
        bool already_in_flight;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            already_in_flight = in_flight_;
            if (!already_in_flight) {
                in_flight_      = true;
                started_        = std::chrono::steady_clock::now();
                slow_reported_  = false;
                budget_expired_ = false;
                paint_received_ = false;
                force_accept_   = false;
                sync_deadline_.reset();
            }
        }
        if (already_in_flight)
            return await(deadline);

        const double budget_ms =
            static_cast<double>(virtual_time_us_at(frame_count_ + 1) - virtual_time_us_at(frame_count_)) / 1000.0;
        ++frame_count_;

        // Only has to differ from the frame before; well spread so it is obvious in a dump.
        const uint8_t mr = static_cast<uint8_t>((frame_count_ * 97 + 13) & 0xFF);
        const uint8_t mg = static_cast<uint8_t>((frame_count_ * 149 + 71) & 0xFF);
        const uint8_t mb = static_cast<uint8_t>((frame_count_ * 211 + 167) & 0xFF);

        html::begin_invoke([this, budget_ms, mr, mg, mb] {
            if (aborted_ || !browser_)
                return;

            expected_marker_[0] = mr;
            expected_marker_[1] = mg;
            expected_marker_[2] = mb;
            begin_frames_       = 0;
            paints_             = 0;

            // pauseIfNetworkFetchesPending so an in-flight fetch defers the frame rather than
            // letting the page see an inconsistent clock. Expiry is reported to on_budget_expired().
            auto vt = CefDictionaryValue::Create();
            vt->SetString("policy", "pauseIfNetworkFetchesPending");
            vt->SetDouble("budget", budget_ms);
            browser_->GetHost()->ExecuteDevToolsMethod(0, "Emulation.setVirtualTimePolicy", vt);
        });

        return await(deadline);
    }

    bool await(std::chrono::steady_clock::time_point deadline)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (!cv_.wait_until(lock, deadline, [&] { return budget_expired_ || aborted_; })) {
                report_slow_locked();
                return false;
            }
            if (aborted_)
                return true;

            // Timed from the clock reaching the frame, not from this call, which waits one slice.
            if (!sync_deadline_)
                sync_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(SYNC_TIMEOUT_MS);
        }

        // Pump begin-frames until a paint carries this frame's marker. They are paced: the
        // renderer needs a few ms to composite what the clock just committed, and a begin-frame
        // sent meanwhile delivers the frame before. The clock stays paused throughout, so only
        // the number of discarded paints varies between runs, not the frame itself.
        //
        // No Invalidate here. Forcing full damage makes every begin-frame re-deliver the buffer
        // the page already has, which is the staleness the marker then has to filter out: over a
        // 500-frame render it turned 23 discarded paints into 1011, and made the first paint of
        // a frame never the right one. Damage comes from the marker instead -- its colour changes
        // every frame -- so a paint still arrives for a frame in which the page itself drew
        // nothing, and the caller never has to tell that apart from a frame that is merely late.
        while (std::chrono::steady_clock::now() < deadline) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (paint_received_ || aborted_)
                    return true;

                if (std::chrono::steady_clock::now() >= *sync_deadline_)
                    force_accept_ = true;
            }

            html::begin_invoke([this] {
                if (aborted_ || !browser_)
                    return;
                ++begin_frames_;
                browser_->GetHost()->SendExternalBeginFrame();
            });

            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(SYNC_PUMP_MS), [&] { return paint_received_ || aborted_; });
        }

        std::lock_guard<std::mutex> lock(mutex_);
        report_slow_locked();
        return false;
    }

    // A page that never lets its clock reach the frame stalls the render, and this is the only
    // sign of it. Once per frame. Call locked.
    void report_slow_locked()
    {
        if (slow_reported_ || std::chrono::steady_clock::now() - started_ < std::chrono::seconds(2))
            return;

        slow_reported_ = true;
        CASPAR_LOG(warning) << name_ << L" [vtc] frame " << frame_count_ << L" still waiting after 2s (budget "
                            << (budget_expired_ ? L"expired" : L"pending") << L", begin-frames=" << begin_frames_.load()
                            << L", paints=" << paints_.load() << L")";
    }

    void on_budget_expired()
    {
        // Stamp the marker now the clock has reached the frame: stamped before advancing it, a
        // main frame could commit the marker while still drawing the frame before, and that
        // stale paint would pass the check below.
        stamp_marker();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            budget_expired_ = true;
        }
        cv_.notify_all();
    }

    void stamp_marker()
    {
        if (aborted_ || !browser_)
            return;

        // Via Runtime.evaluate on the DevTools pipe: CefFrame::ExecuteJavaScript takes another
        // path and lands late. Self-contained, so it also re-creates the element if the page
        // wiped the DOM.
        const std::string rgb = "rgb(" + std::to_string(expected_marker_[0]) + "," +
                                std::to_string(expected_marker_[1]) + "," + std::to_string(expected_marker_[2]) + ")";
        const std::string expr = "(function(c){var m=document.getElementById('__caspar_mark');"
                                 "if(!m){m=document.createElement('div');m.id='__caspar_mark';"
                                 "m.style.cssText='position:fixed;left:0;top:0;width:1px;height:1px;"
                                 "margin:0;padding:0;border:0;z-index:2147483647;pointer-events:none';"
                                 "document.documentElement.appendChild(m);}m.style.background=c;})('" +
                                 rgb + "')";

        auto ev = CefDictionaryValue::Create();
        ev->SetString("expression", expr);
        browser_->GetHost()->ExecuteDevToolsMethod(0, "Runtime.evaluate", ev);
    }

    bool accept_paint(const void* buffer, int width, int height)
    {
        ++paints_;

        // The marker is opaque and unscaled, so it reaches us byte for byte: match it exactly.
        const auto* buf     = static_cast<const unsigned char*>(buffer);
        bool        matched = false;

        if (buf != nullptr && width > 0 && height > 0)
            matched = buf[2] == expected_marker_[0] && buf[1] == expected_marker_[1] &&
                      buf[0] == expected_marker_[2];

        // Once per frame, only after its clock has arrived, and only on a match (or once we
        // give up waiting for one). Paints are serialized on the UI thread, so the next one
        // sees paint_received_ and is discarded.
        std::lock_guard<std::mutex> lock(mutex_);
        return budget_expired_ && !paint_received_ && (matched || force_accept_);
    }

    void erase_marker(char* image, int width, int height)
    {
        // Overwritten with the pixel to its right: a single-pixel smear in the corner.
        if (width > 1 && height > 0)
            std::memcpy(image, image + 4, 4);
    }

    void frame_queued()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            paint_received_ = true;
            in_flight_      = false;
        }
        cv_.notify_all();
    }

    void abort()
    {
        aborted_ = true;
        cv_.notify_all();
    }
};

virtual_time_driver::virtual_time_driver(const core::video_format_desc& format_desc, std::wstring name)
    : impl_(std::make_unique<impl>(format_desc, std::move(name)))
{
}

virtual_time_driver::~virtual_time_driver() { impl_->abort(); }

void virtual_time_driver::attach(CefRefPtr<CefBrowser> browser) { impl_->browser_ = std::move(browser); }
void virtual_time_driver::abort() { impl_->abort(); }
void virtual_time_driver::pause_clock() { impl_->pause_clock(); }
bool virtual_time_driver::tick(std::chrono::steady_clock::time_point deadline) { return impl_->tick(deadline); }
void virtual_time_driver::on_budget_expired() { impl_->on_budget_expired(); }
void virtual_time_driver::frame_queued() { impl_->frame_queued(); }

bool virtual_time_driver::accept_paint(const void* buffer, int width, int height)
{
    return impl_->accept_paint(buffer, width, height);
}

void virtual_time_driver::erase_marker(char* image, int width, int height)
{
    impl_->erase_marker(image, width, height);
}

}} // namespace caspar::html
