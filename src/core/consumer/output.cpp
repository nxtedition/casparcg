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
#include "output.h"

#include "channel_info.h"
#include "frame_consumer.h"

#include "../frame/frame.h"
#include "../frame/pixel_format.h"

#include <common/bit_depth.h>
#include <common/diagnostics/graph.h>
#include <common/except.h>
#include <common/memory.h>

#include <map>
#include <utility>

namespace caspar { namespace core {

struct output::impl
{
    monitor::state                      state_;
    spl::shared_ptr<diagnostics::graph> graph_;
    const channel_info                  channel_info_;
    video_format_desc                   format_desc_;

    std::mutex                                     consumers_mutex_;
    std::map<int, spl::shared_ptr<frame_consumer>> consumers_;

    const spl::shared_ptr<channel_pacing> pacing_;

  public:
    impl(const spl::shared_ptr<diagnostics::graph>& graph,
         const video_format_desc&                   format_desc,
         const core::channel_info&                  channel_info,
         spl::shared_ptr<channel_pacing>            pacing)
        : graph_(graph)
        , channel_info_(channel_info)
        , format_desc_(format_desc)
        , pacing_(std::move(pacing))
    {
    }

    void add(int index, spl::shared_ptr<frame_consumer> consumer)
    {
        if (channel_info_.deterministic && !consumer->supports_deterministic_sync()) {
            CASPAR_THROW_EXCEPTION(user_error()
                                   << msg_info(L"Cannot attach a consumer that drops frames to a deterministic "
                                               L"channel: " +
                                               consumer->print()));
        }

        // Taken out quietly, and reported once the new one is in: losing the last consumer ends
        // a deterministic render, and a replacement must not look like that for an instant.
        bool replaced;
        {
            std::lock_guard<std::mutex> lock(consumers_mutex_);
            replaced = consumers_.erase(index) > 0;
        }

        try {
            consumer->initialize(format_desc_, channel_info_, index);
        } catch (...) {
            if (replaced) {
                std::lock_guard<std::mutex> lock(consumers_mutex_);
                report_consumers_locked();
            }
            throw;
        }

        std::lock_guard<std::mutex> lock(consumers_mutex_);
        consumers_.emplace(index, std::move(consumer));
        report_consumers_locked();
    }

    void add(const spl::shared_ptr<frame_consumer>& consumer) { add(consumer->index(), consumer); }

    bool remove(int index)
    {
        std::lock_guard<std::mutex> lock(consumers_mutex_);
        if (consumers_.erase(index) == 0)
            return false;

        report_consumers_locked();
        return true;
    }

    void change_format(const core::video_format_desc& format_desc)
    {
        std::lock_guard<std::mutex> lock(consumers_mutex_);
        for (auto it = consumers_.begin(); it != consumers_.end();) {
            try {
                it->second->initialize(format_desc, channel_info_, it->first);
                ++it;
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
                it = consumers_.erase(it);
            }
        }
        report_consumers_locked(); // some may have failed to re-initialize and been dropped
        format_desc_ = format_desc;
        pacing_->reset();
    }

    // Call with consumers_mutex_ held, so reports reach the strategy in the order the changes
    // were made: a stale report of zero would end a render that still has a consumer.
    void report_consumers_locked() { pacing_->consumers_changed(consumers_.size()); }

    bool remove(const spl::shared_ptr<frame_consumer>& consumer) { return remove(consumer->index()); }

    std::future<bool> call(int index, const std::vector<std::wstring>& params)
    {
        std::lock_guard<std::mutex> lock(consumers_mutex_);
        auto                        it = consumers_.find(index);
        if (it != consumers_.end()) {
            try {
                return it->second->call(params);
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
            }
        } else {
            CASPAR_LOG(warning) << print() << L" No consumer found for index " << index << L".";
        }
        return caspar::make_ready_future(false);
    }

    size_t consumer_count()
    {
        std::lock_guard<std::mutex> lock(consumers_mutex_);
        return consumers_.size();
    }

    void operator()(const const_frame&             input_frame1,
                    const const_frame&             input_frame2,
                    const core::video_format_desc& format_desc)
    {
        if (format_desc_ != format_desc) {
            change_format(format_desc);
            return;
        }

        // If no frame is provided, this should only happen when the channel has no consumers.
        // Take a shortcut and perform the sleep to let the channel tick correctly.
        if (!input_frame1) {
            pacing_->tick(format_desc_);
            return;
        }

        const auto bytesPerComponent1 =
            input_frame1.pixel_format_desc().planes.at(0).depth == common::bit_depth::bit8 ? 1 : 2;
        if (input_frame1.size() != format_desc_.size * bytesPerComponent1) {
            CASPAR_LOG(warning) << print() << L" Invalid input frame size.";
            pacing_->reset();
            return;
        }

        if (input_frame2) {
            const auto bytesPerComponent2 =
                input_frame2.pixel_format_desc().planes.at(0).depth == common::bit_depth::bit8 ? 1 : 2;

            if (input_frame2.size() != format_desc_.size * bytesPerComponent2) {
                CASPAR_LOG(warning) << print() << L" Invalid input frame size.";
                pacing_->reset();
                return;
            }
        }

        decltype(consumers_) consumers;
        {
            std::lock_guard<std::mutex> lock(consumers_mutex_);
            consumers = consumers_;
        }

        // Callers stay responsible for the local `consumers` copy, so the iterator handling
        // below is unchanged.
        auto drop_consumer = [this](int index) {
            std::lock_guard<std::mutex> lock(consumers_mutex_);
            if (consumers_.erase(index) > 0)
                report_consumers_locked();
        };

        auto do_send = [&](core::video_field field, const core::const_frame& frame) {
            std::map<int, std::future<bool>> futures;

            for (auto it = consumers.begin(); it != consumers.end();) {
                try {
                    futures.emplace(it->first, it->second->send(field, frame));
                    ++it;
                } catch (...) {
                    CASPAR_LOG_CURRENT_EXCEPTION();
                    auto index = it->first;
                    it         = consumers.erase(it);
                    drop_consumer(index);
                }
            }

            for (auto& p : futures) {
                try {
                    if (!p.second.get()) {
                        consumers.erase(p.first);
                        drop_consumer(p.first);
                    }
                } catch (...) {
                    CASPAR_LOG_CURRENT_EXCEPTION();
                    consumers.erase(p.first);
                    drop_consumer(p.first);
                }
            }
        };

        if (format_desc_.field_count == 2) {
            do_send(core::video_field::a, input_frame1);
            do_send(core::video_field::b, input_frame2);
        } else {
            do_send(core::video_field::progressive, input_frame1);
        }

        monitor::state state;
        for (auto& p : consumers) {
            state["port"][p.first]             = p.second->state();
            state["port"][p.first]["consumer"] = p.second->name();
        }
        state_ = std::move(state);

        const auto needs_sync = std::all_of(
            consumers.begin(), consumers.end(), [](auto& p) { return !p.second->has_synchronization_clock(); });

        if (needs_sync) {
            pacing_->tick(format_desc_);
        } else {
            // A consumer brings its own clock; its blocking send() is what paces us.
            pacing_->reset();
        }
    }

    std::wstring print() const { return L"output[" + std::to_wstring(channel_info_.index) + L"]"; }
};

output::output(const spl::shared_ptr<diagnostics::graph>& graph,
               const video_format_desc&                   format_desc,
               const core::channel_info&                  channel_info,
               spl::shared_ptr<channel_pacing>            pacing)
    : impl_(new impl(graph, format_desc, channel_info, std::move(pacing)))
{
}
output::~output() {}
void output::add(int index, const spl::shared_ptr<frame_consumer>& consumer) { impl_->add(index, consumer); }
void output::add(const spl::shared_ptr<frame_consumer>& consumer) { impl_->add(consumer); }
bool output::remove(int index) { return impl_->remove(index); }
bool output::remove(const spl::shared_ptr<frame_consumer>& consumer) { return impl_->remove(consumer); }
std::future<bool> output::call(int index, const std::vector<std::wstring>& params)
{
    return impl_->call(index, params);
}
size_t output::consumer_count() const { return impl_->consumer_count(); }
void   output::change_format(const video_format_desc& format_desc) { impl_->change_format(format_desc); }
void   output::operator()(const const_frame& frame, const const_frame& frame2, const video_format_desc& format_desc)
{
    return (*impl_)(frame, frame2, format_desc);
}
core::monitor::state output::state() const { return impl_->state_; }
}} // namespace caspar::core
