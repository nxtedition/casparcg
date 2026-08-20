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

// The platform-independent half of the accelerated paint path: caching, the copy, frame
// assembly, and the semaphore lifecycle. Everything that differs per platform is behind
// accelerated_paint_platform.h, implemented by exactly one accelerated_paint_<platform>.cpp.
//
// This file is only compiled when such an implementation exists; otherwise CMakeLists.txt
// substitutes accelerated_paint_null.cpp, which reports the path as unusable.

#include "accelerated_paint_importer.h"

#include "accelerated_paint_platform.h"

#include <common/log.h>

#include <core/frame/frame_factory.h>
#include <core/frame/pixel_format.h>

#include <accelerator/vulkan/util/gpu_producer.h>
#include <accelerator/vulkan/util/texture.h>
#include <accelerator/vulkan/util/vulkan_queue.h>

#include <algorithm>
#include <deque>

namespace caspar { namespace html {

using namespace accelerator::vulkan;

struct accelerated_paint_importer::impl
{
    // One imported source texture, kept across frames. CEF cycles a small ring of buffers, so
    // after the first frames every callback hits this cache and the per-frame work is one
    // command buffer.
    struct entry
    {
        uint64_t                        key    = 0;
        uint64_t                        layout = 0;
        int                             width  = 0;
        int                             height = 0;
        vk::Format                      format = vk::Format::eUndefined;
        std::shared_ptr<imported_image> image;
    };

    // CEF cycles far fewer buffers than this; the cap only bounds a pathological exporter.
    static constexpr size_t max_cached_imports = 8;

    // The copy is tens of microseconds of GPU work. A whole second means something is wrong,
    // and blocking the browser's UI thread forever would be worse than a dropped frame.
    static constexpr uint64_t copy_timeout_ns = 1'000'000'000ull;

    // A binary semaphore's signal belongs to exactly one submit, so neither half of a pair can
    // be reused or destroyed until that submit has completed. Since the whole point of this
    // path is returning with the copy still in flight, they outlive the callback and have to
    // be retired by completion token rather than at the end of the frame.
    struct pending_sync
    {
        completion_token token;
        vk::Semaphore    wait   = nullptr;
        vk::Semaphore    signal = nullptr;
    };

    // Deep enough to cover CEF's buffer pool without letting an unretired token pile up.
    static constexpr size_t max_pending_sync = 8;

    gpu_producer             gpu_;
    std::deque<entry>        cache_;
    std::deque<pending_sync> pending_sync_;
    std::wstring             unusable_reason_;
    bool                     usable_   = false;
    bool                     failed_   = false;
    bool                     imported_ = false;

    // Set once if the kernel ever refuses our release fence; see the publish site.
    bool reported_release_failure_ = false;

    explicit impl(const spl::shared_ptr<core::frame_factory>& frame_factory)
        // We're going to do a plain GPU-GPU copy, to a texture to be consumed on the graphics queue,
        // easiest to use the graphics queue directly
        : gpu_(frame_factory, queue_type::graphics)
    {
        if (!gpu_) {
            unusable_reason_ = L"the channel is not on the Vulkan accelerator";
            return;
        }
        unusable_reason_ = platform::unusable_reason(gpu_.factory());
        usable_          = unusable_reason_.empty();
    }

    ~impl()
    {
        if (!gpu_)
            return;
        // The semaphores below are still referenced by submits that may be in flight.
        gpu_.context().wait_idle(copy_timeout_ns);
        for (auto& p : pending_sync_) {
            gpu_.factory().destroy_semaphore(p.wait);
            gpu_.factory().destroy_semaphore(p.signal);
        }
    }

    core::const_frame import(const void* tag, const CefAcceleratedPaintInfo& info, int dst_width, int dst_height)
    {
        if (!usable_)
            return {};

        vk::Format         src_format;
        core::pixel_format frame_format;
        if (!map_format(info.format, src_format, frame_format)) {
            fail(L"shared texture has an unsupported pixel format");
            return {};
        }

        // coded_size is the allocation, which the compositor may pad; visible_rect is the part
        // of it the page actually occupies. Both are optional, so fall back to the view size.
        const int src_width  = info.extra.coded_size.width > 0 ? info.extra.coded_size.width : dst_width;
        const int src_height = info.extra.coded_size.height > 0 ? info.extra.coded_size.height : dst_height;

        auto visible = info.extra.visible_rect;
        if (visible.width <= 0 || visible.height <= 0) {
            visible.x      = 0;
            visible.y      = 0;
            visible.width  = src_width;
            visible.height = src_height;
        }

        const int copy_width  = std::min({visible.width, src_width - visible.x, dst_width});
        const int copy_height = std::min({visible.height, src_height - visible.y, dst_height});
        if (copy_width <= 0 || copy_height <= 0)
            return {};

        auto src = acquire_image(info, src_width, src_height, src_format);
        if (!src)
            return {};

        // Retire the semaphores of frames the GPU has already finished with, so the pair
        // allocated below is the only one this frame adds.
        reclaim_sync();

        platform::frame_sync sync;
        sync.semaphores.wait = platform::acquire_wait(gpu_.factory(), info);
        if (sync.semaphores.wait)
            sync.semaphores.wait_stage = vk::PipelineStageFlagBits::eTransfer;
        sync.semaphores.signal = gpu_.factory().create_exportable_semaphore();

        auto dst_tex = gpu_.factory().create_producer_texture(dst_width, dst_height, 4, common::bit_depth::bit8);

        const uint32_t family = gpu_.context().queue()->family_index();

        auto frame = gpu_.produce(
            tag,
            producer_plane{std::move(dst_tex)},
            frame_format,
            [&](vk::CommandBuffer cmd, const std::shared_ptr<texture>& dst) {
                // The browser's GPU process wrote this memory since we last saw it, so take
                // the image over from it every frame, not just on the first.
                record_foreign_acquire(cmd,
                                       src->id(),
                                       family,
                                       vk::ImageLayout::eTransferSrcOptimal,
                                       vk::PipelineStageFlagBits2::eTransfer,
                                       vk::AccessFlagBits2::eTransferRead);

                vk::ImageCopy region{};
                region.srcSubresource = vk::ImageSubresourceLayers(vk::ImageAspectFlagBits::eColor, 0, 0, 1);
                region.srcOffset      = vk::Offset3D{visible.x, visible.y, 0};
                region.dstSubresource = region.srcSubresource;
                region.dstOffset      = vk::Offset3D{0, 0, 0};
                region.extent = vk::Extent3D{static_cast<uint32_t>(copy_width), static_cast<uint32_t>(copy_height), 1};

                // eGeneral is the layout gpu_producer's acquire left `dst` in.
                cmd.copyImage(
                    src->id(), vk::ImageLayout::eTransferSrcOptimal, dst->id(), vk::ImageLayout::eGeneral, region);

                // Hand it back, so the browser may refill the buffer once our read has landed.
                record_foreign_release(cmd,
                                       src->id(),
                                       family,
                                       vk::ImageLayout::eTransferSrcOptimal,
                                       vk::PipelineStageFlagBits2::eTransfer,
                                       vk::AccessFlagBits2::eTransferRead);
            },
            {},
            sync.semaphores);

        // CEF releases the buffer back to its pool the moment this callback returns, so
        // something has to stop the browser refilling it while our copy is still reading.
        const bool published = platform::publish_release(gpu_.factory(), info, sync.semaphores.signal);

        // Both semaphores are still referenced by the submit above, so they cannot be
        // destroyed here however this turns out.
        pending_sync_.push_back(
            pending_sync{gpu_.context().current_completion(), sync.semaphores.wait, sync.semaphores.signal});

        if (!published) {
            // The device capability was checked at construction, so reaching here means the
            // platform refused at runtime. There is no going back to OnPaint at this point —
            // the browser was created for shared textures — so hold the buffer by blocking
            // instead, which is correct everywhere and costs the browser's UI thread a few
            // hundred microseconds a frame.
            if (!reported_release_failure_) {
                reported_release_failure_ = true;
                CASPAR_LOG(error) << L"html: " << platform::release_failure_hint();
            }
            if (!gpu_.context().wait_idle(copy_timeout_ns)) {
                CASPAR_LOG(warning) << L"html: timed out waiting for the shared texture copy.";
                return {};
            }
        }

        return frame;
    }

    // CEF's pixel format to ours. The copy is a raw texel-block copy into an RGBA8 texture, so
    // the bytes stay in the browser's order and the mixer's shader does the swizzle — exactly
    // what the OnPaint path relies on for the same buffer.
    static bool map_format(cef_color_type_t cef, vk::Format& src, core::pixel_format& frame)
    {
        switch (cef) {
            case CEF_COLOR_TYPE_BGRA_8888:
                src   = vk::Format::eB8G8R8A8Unorm;
                frame = core::pixel_format::bgra;
                return true;
            case CEF_COLOR_TYPE_RGBA_8888:
                src   = vk::Format::eR8G8B8A8Unorm;
                frame = core::pixel_format::rgba;
                return true;
            default:
                return false;
        }
    }

    // The imported image for this frame's shared texture, from the cache when we have seen the
    // buffer before.
    std::shared_ptr<imported_image>
    acquire_image(const CefAcceleratedPaintInfo& info, int width, int height, vk::Format format)
    {
        const uint64_t key = platform::source_key(info);
        if (key == 0)
            return nullptr;
        const uint64_t layout = platform::source_layout(info);

        for (auto it = cache_.begin(); it != cache_.end(); ++it) {
            if (it->key == key && it->layout == layout && it->width == width && it->height == height &&
                it->format == format) {
                auto hit = *it;
                cache_.erase(it);
                cache_.push_front(hit);
                return hit.image;
            }
        }

        auto image = platform::import_source(gpu_.factory(), info, width, height, format);
        if (!image) {
            fail(L"could not import the shared texture");
            return nullptr;
        }

        if (!imported_) {
            imported_ = true;
            platform::log_first_import(info, width, height);
        }

        cache_.push_front(entry{key, layout, width, height, format, image});
        while (cache_.size() > max_cached_imports)
            cache_.pop_back();

        return image;
    }

    // Destroy the semaphores of every submit the GPU has already passed. Polls with a zero
    // timeout — this runs on the browser's UI thread and must not block it.
    void reclaim_sync()
    {
        while (!pending_sync_.empty() && gpu_.context().wait(pending_sync_.front().token, 0)) {
            gpu_.factory().destroy_semaphore(pending_sync_.front().wait);
            gpu_.factory().destroy_semaphore(pending_sync_.front().signal);
            pending_sync_.pop_front();
        }

        // Backstop: if the queue is not draining we are leaking semaphores, so block once and
        // clear it out rather than growing without bound.
        if (pending_sync_.size() >= max_pending_sync) {
            gpu_.context().wait_idle(copy_timeout_ns);
            for (auto& p : pending_sync_) {
                gpu_.factory().destroy_semaphore(p.wait);
                gpu_.factory().destroy_semaphore(p.signal);
            }
            pending_sync_.clear();
        }
    }

    // Report the first failure loudly and stay quiet after that. There is no falling back from
    // here: the browser was created asking for shared textures, so CEF will not start calling
    // OnPaint instead — recovering means restarting with the path turned off.
    void fail(const std::wstring& what)
    {
        if (failed_)
            return;
        failed_ = true;
        CASPAR_LOG(error) << L"html: " << what
                          << L" — the accelerated paint path cannot produce frames. Set "
                             L"<enable-accelerated-paint>false</enable-accelerated-paint> under <html> to fall back "
                             L"to CPU painting.";
    }
};
accelerated_paint_importer::accelerated_paint_importer(const spl::shared_ptr<core::frame_factory>& frame_factory)
    : impl_(std::make_unique<impl>(frame_factory))
{
}

accelerated_paint_importer::~accelerated_paint_importer() = default;

bool accelerated_paint_importer::usable() const { return impl_->usable_; }

const std::wstring& accelerated_paint_importer::unusable_reason() const { return impl_->unusable_reason_; }

core::const_frame
accelerated_paint_importer::import(const void* tag, const CefAcceleratedPaintInfo& info, int dst_width, int dst_height)
{
    return impl_->import(tag, info, dst_width, dst_height);
}

}} // namespace caspar::html
