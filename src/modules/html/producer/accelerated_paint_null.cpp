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

// Substituted for accelerated_paint_importer.cpp + accelerated_paint_<platform>.cpp on builds
// that have no way to consume CEF's shared texture: no Vulkan accelerator, or a platform whose
// backend is not written yet (see accelerated_paint_platform.h for what one has to provide).
//
// The type still exists and still answers usable(), so html_producer.cpp needs no #ifdef and
// the operator gets told why the path was refused rather than being left to infer it.

#include "accelerated_paint_importer.h"

namespace caspar { namespace html {

struct accelerated_paint_importer::impl
{
    std::wstring unusable_reason_ =
#if !defined(ENABLE_VULKAN)
        L"this build has no Vulkan accelerator";
#else
        L"consuming CEF shared textures is not implemented on this platform yet";
#endif
    bool usable_ = false;

    explicit impl(const spl::shared_ptr<core::frame_factory>&) {}

    core::const_frame import(const void*, const CefAcceleratedPaintInfo&, int, int) { return {}; }
};

accelerated_paint_importer::accelerated_paint_importer(const spl::shared_ptr<core::frame_factory>& frame_factory)
    : impl_(std::make_unique<impl>(frame_factory))
{
}

accelerated_paint_importer::~accelerated_paint_importer() = default;

bool accelerated_paint_importer::usable() const { return impl_->usable_; }

const std::wstring& accelerated_paint_importer::unusable_reason() const { return impl_->unusable_reason_; }

core::const_frame accelerated_paint_importer::import(const void*                    tag,
                                                     const CefAcceleratedPaintInfo& info,
                                                     int                            dst_width,
                                                     int                            dst_height)
{
    return impl_->import(tag, info, dst_width, dst_height);
}

}} // namespace caspar::html
