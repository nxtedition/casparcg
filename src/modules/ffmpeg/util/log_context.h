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

#include <common/utf.h>

#include <string>

namespace caspar { namespace ffmpeg {

// Concrete logging-context data. A single instance is owned by AVProducer::Impl;
// the Input and Decoder threads hold a pointer to it and hand it to
// set_thread_log_context(). The to_log_string() overload (resolved via ADL by the
// log_context carrier below) defines how it is rendered into each log line.
struct log_context_data
{
    std::string name;
};

inline std::wstring to_log_string(const log_context_data& data) { return L"ffmpeg " + u16(data.name); }

// Type-erased carrier for a logging context. Holds an arbitrary object pointer
// together with a function that renders it to a wstring. The render function is
// resolved via ADL to a free `to_log_string(const T&)` overload, so any type can
// be made loggable simply by providing that overload in its namespace.
//
// The pointer is rendered lazily (on each read), so a context set before its
// fields are fully populated still reflects the latest state when logged.
class log_context
{
    const void* obj_                  = nullptr;
    std::wstring (*fmt_)(const void*) = nullptr;

  public:
    log_context() = default;

    template <typename T>
    log_context(const T* obj)
        : obj_(obj)
        , fmt_(+[](const void* p) { return to_log_string(*static_cast<const T*>(p)); })
    {
    }

    explicit operator bool() const { return obj_ != nullptr; }

    std::wstring str() const { return obj_ != nullptr ? fmt_(obj_) : std::wstring{}; }
};

// Per-thread logging context. Set it once on each thread that drives ffmpeg; the
// ffmpeg log callback reads it to tag every log line emitted from that thread.
void               set_thread_log_context(log_context ctx);
const log_context& thread_log_context();

}} // namespace caspar::ffmpeg
