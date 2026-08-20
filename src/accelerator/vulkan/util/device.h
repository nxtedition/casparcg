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

#include "dmabuf.h"
#include "queue_manager.h"

#include <accelerator/accelerator.h>
#include <common/array.h>
#include <common/bit_depth.h>
#include <core/frame/geometry.h>

#include <future>

#include <vulkan/vulkan.hpp>

namespace caspar { namespace accelerator { namespace vulkan {

struct draw_params;

class image_kernel;
class vulkan_queue;
class transfer;

class device final
    : public std::enable_shared_from_this<device>
    , public accelerator_device
{
  public:
    explicit device(const std::vector<vulkan_requirements_fn>& requirements = {});
    ~device();

    device(const device&) = delete;

    device& operator=(const device&) = delete;

    vk::PhysicalDeviceMemoryProperties getMemoryProperties();
    vk::Device                         getVkDevice() const;
    vk::Instance                       instance() const;
    vk::PhysicalDevice                 physical_device() const;
    std::shared_ptr<vulkan_queue>      queue();
    // Hand out the queue dedicated to a kind of work (transfer/compute/video), so a
    // client (e.g. the screen consumer, hw decode) can run off the render queue.
    // Transfer/compute collapse to queue() on hardware without a dedicated family;
    // a video type returns nullptr when the hardware can't do it. Internally
    // synchronized and shared — no reclamation, exhaustion is impossible.
    std::shared_ptr<vulkan_queue> acquire_queue(queue_type type);
    class transfer&               transfer();

    std::shared_ptr<class texture> create_texture(int width, int height, int stride, common::bit_depth depth);
    std::shared_ptr<class buffer>  create_buffer(int size, bool write);
    array<uint8_t>                 create_array(int size);

    // True when the device came up with the whole DMA-BUF import extension set
    // (external_memory_fd + external_memory_dma_buf + image_drm_format_modifier +
    // queue_family_foreign). A module that wants to import must ask for those in its
    // vulkan_requirements_fn — this only reports what actually got enabled, because a
    // module cannot know whether some other GPU won the device selection.
    bool supports_dmabuf_import() const;

    // Import an externally allocated DMA-BUF as a transfer-source VkImage. Returns null
    // (never throws) when the device lacks the extensions, when the exporter did not name
    // a DRM format modifier, or when the driver rejects that format/modifier/plane layout
    // — every one of those is a "fall back to the CPU path" answer, not a fatal error.
    // The caller keeps ownership of the fds in `img`; anything kept is dup'd.
    std::shared_ptr<imported_image> import_dmabuf(const dmabuf_image& img);

    // True when the device can move a sync_file fd in and out of a VkSemaphore
    // (VK_KHR_external_semaphore_fd, with the SYNC_FD handle type reported importable AND
    // exportable by the driver). Together with the dma-buf sync_file ioctls this replaces
    // blocking the CPU on an imported frame — see accelerator/vulkan/util/dmabuf.h.
    bool supports_sync_fd_semaphores() const;

    // Wrap a sync_file fd in a binary semaphore a submit can wait on. Takes OWNERSHIP of
    // `sync_fd` on success (Vulkan closes it); the caller keeps it on failure. The import is
    // temporary: after the wait consumes it the semaphore is unsignalled again and can be
    // reused for the next frame. Returns null (never throws) if unsupported or refused.
    vk::Semaphore import_sync_fd_semaphore(int sync_fd);

    // A binary semaphore that a later vkGetSemaphoreFdKHR may export as a sync_file. Must be
    // created up front with the export handle type — an ordinary semaphore cannot be exported.
    vk::Semaphore create_exportable_semaphore();

    // Turn a semaphore's PENDING signal operation into a sync_file fd the caller owns and
    // must close. Legal — and only useful — after the submit that signals it has been
    // queued; the fd signals when that work completes. -1 on failure.
    int export_sync_fd(vk::Semaphore semaphore);

    void destroy_semaphore(vk::Semaphore semaphore);

    std::wstring version() const;

    boost::property_tree::wptree info() const;
    std::future<void>            gc();

  private:
    struct impl;
    std::shared_ptr<impl> impl_;
};

}}} // namespace caspar::accelerator::vulkan
