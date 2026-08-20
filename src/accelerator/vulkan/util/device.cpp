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

#include "device.h"

#include "../image/image_kernel.h"
#include "buffer.h"
#include "dmabuf.h"
#include "queue_manager.h"
#include "texture.h"
#include "transfer.h"
#include "vulkan_queue.h"

#include <common/array.h>
#include <common/assert.h>
#include <common/env.h>
#include <common/except.h>
#include <common/future.h>

#include <VkBootstrap.h>
#include <vulkan/vulkan.hpp>

VULKAN_HPP_DEFAULT_DISPATCH_LOADER_DYNAMIC_STORAGE

#define VMA_STATIC_VULKAN_FUNCTIONS 0
#define VMA_DYNAMIC_VULKAN_FUNCTIONS 1
#define VMA_IMPLEMENTATION
#pragma warning(push)
#pragma warning(disable : 4189)
#include <vk_mem_alloc.h>
#pragma warning(pop)

#include <boost/property_tree/ptree.hpp>

#ifndef _WIN32
#include <unistd.h> // dup/close, for the DMA-BUF fd Vulkan takes ownership of
#endif

#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_unordered_map.h>

#include <algorithm>
#include <array>
#include <deque>
#include <future>
#include <memory>
#include <string>

namespace caspar { namespace accelerator { namespace vulkan {

inline VKAPI_ATTR VkBool32 VKAPI_CALL default_debug_callback(VkDebugUtilsMessageSeverityFlagBitsEXT messageSeverity,
                                                             VkDebugUtilsMessageTypeFlagsEXT        messageType,
                                                             const VkDebugUtilsMessengerCallbackDataEXT* pCallbackData,
                                                             void*)
{
    auto ms = vkb::to_string_message_severity(messageSeverity);
    auto mt = vkb::to_string_message_type(messageType);
    if (messageType & VK_DEBUG_UTILS_MESSAGE_TYPE_VALIDATION_BIT_EXT) {
        CASPAR_LOG(info) << "[" << ms << ": " << mt << "] - " << pCallbackData->pMessageIdName << ", "
                         << pCallbackData->pMessage;
        // printf("[%s: %s] - %s\n%s\n", ms, mt, pCallbackData->pMessageIdName, pCallbackData->pMessage);
    } else {
        if (pCallbackData->pMessage != nullptr) {
            CASPAR_LOG(info) << "[" << ms << ": " << mt << "] " << pCallbackData->pMessage;
            // printf("[%s: %s]\n%s\n", ms, mt, pCallbackData->pMessage);
        }
    }

    return VK_FALSE; // Applications must return false here (Except Validation, if return true, will skip calling to
                     // driver)
}

namespace {
// dup/close of a DMA-BUF fd. Only ever reached on POSIX — DMA-BUF import needs
// VK_EXT_external_memory_dma_buf, which no Windows driver exposes — but the import code is
// compiled everywhere, so give the calls somewhere to land.
#ifdef _WIN32
inline int  dup_dmabuf_fd(int) { return -1; }
inline void close_dmabuf_fd(int) {}
#else
inline int  dup_dmabuf_fd(int fd) { return ::dup(fd); }
inline void close_dmabuf_fd(int fd) { ::close(fd); }
#endif
} // namespace

struct device::impl : public std::enable_shared_from_this<impl>
{
    using texture_queue_t = tbb::concurrent_bounded_queue<std::shared_ptr<texture>>;
    using buffer_queue_t  = tbb::concurrent_bounded_queue<std::shared_ptr<buffer>>;

    std::array<std::array<tbb::concurrent_unordered_map<size_t, texture_queue_t>, 4>, 2> device_pools_;
    std::array<tbb::concurrent_unordered_map<size_t, buffer_queue_t>, 2>                 host_pools_;

    std::wstring version_;

    vkb::Instance                      _vkb_instance;
    vkb::PhysicalDevice                _vkb_physical_device;
    vk::PhysicalDeviceMemoryProperties _memoryProperties;
    vk::PhysicalDevice                 _physical_device;
    vk::Device                         _device;
    // Owns the queues: primary() is the render path, acquire(queue_type) hands out
    // the queue dedicated to a kind of work (transfer/compute/video).
    std::unique_ptr<queue_manager> queue_manager_;
    VmaAllocator                   _allocator;

    std::unique_ptr<class transfer> transfer_;

    // Whether the DMA-BUF import extension set survived device creation (see
    // device::supports_dmabuf_import).
    bool dmabuf_import_ = false;

    // Whether sync_file <-> VkSemaphore round-tripping survived device creation (see
    // device::supports_sync_fd_semaphores).
    bool sync_fd_semaphores_ = false;

    explicit impl(const std::vector<vulkan_requirements_fn>& requirements)
    {
        CASPAR_LOG(info) << L"Initializing Vulkan Device.";

        auto instance_builder = vkb::InstanceBuilder()
#ifdef _DEBUG
                                    .enable_validation_layers(true)
                                    .set_debug_messenger_severity(VK_DEBUG_UTILS_MESSAGE_SEVERITY_WARNING_BIT_EXT |
                                                                  VK_DEBUG_UTILS_MESSAGE_SEVERITY_ERROR_BIT_EXT)
                                    .set_debug_messenger_type(VK_DEBUG_UTILS_MESSAGE_TYPE_GENERAL_BIT_EXT |
                                                              VK_DEBUG_UTILS_MESSAGE_TYPE_VALIDATION_BIT_EXT |
                                                              VK_DEBUG_UTILS_MESSAGE_TYPE_PERFORMANCE_BIT_EXT)
                                    .set_debug_callback(default_debug_callback)
#endif
                                    .set_app_name("CasparCG")
                                    .set_headless(true)
                                    .set_engine_name("CasparCG")
#ifdef __APPLE__
                                    .require_api_version(VK_API_VERSION_1_4);
#else
                                    .require_api_version(VK_API_VERSION_1_3);
#endif

        // Enable the surface-creation instance extensions when the loader reports
        // them available, so consumers (e.g. the screen consumer) can present to a
        // window. Enabling the available extensions here is purely additive.
        if (auto sys_info = vkb::SystemInfo::get_system_info()) {
            const char* surface_extensions[] = {
                "VK_KHR_surface",
                "VK_EXT_metal_surface",
                "VK_KHR_win32_surface",
                "VK_KHR_xlib_surface",
                "VK_KHR_xcb_surface",
                "VK_KHR_wayland_surface",
            };
            for (const auto* ext : surface_extensions) {
                if (sys_info->is_extension_available(ext)) {
                    instance_builder.enable_extension(ext);
                }
            }
        }

        auto instance_ret = instance_builder.build();
        if (!instance_ret) {
            CASPAR_THROW_EXCEPTION(caspar_exception()
                                   << msg_info("Failed to create Vulkan instance: " + instance_ret.error().message()));
        }
        _vkb_instance = instance_ret.value();

        VULKAN_HPP_DEFAULT_DISPATCHER.init(_vkb_instance.fp_vkGetInstanceProcAddr);
        VULKAN_HPP_DEFAULT_DISPATCHER.init(vk::Instance(_vkb_instance.instance));

        // Find suitable physical device
        auto gpu_selector = vkb::PhysicalDeviceSelector(_vkb_instance);

        vk::PhysicalDeviceVulkan12Features features12;
        features12.descriptorIndexing                        = true;
        features12.descriptorBindingPartiallyBound           = true;
        features12.runtimeDescriptorArray                    = true;
        features12.shaderSampledImageArrayNonUniformIndexing = true;
        features12.timelineSemaphore                         = true;

        vk::PhysicalDeviceVulkan13Features features13;
        features13.dynamicRendering = true;
        features13.synchronization2 = true;

        vk::PhysicalDeviceDynamicRenderingLocalReadFeaturesKHR localReadFeatures;
        localReadFeatures.dynamicRenderingLocalRead = true;

        auto gpu_res = gpu_selector.set_minimum_version(1, 3)
                           .set_required_features_12(features12)
                           .set_required_features_13(features13)
                           .add_required_extension(VK_KHR_DYNAMIC_RENDERING_LOCAL_READ_EXTENSION_NAME)
                           .add_required_extension_features(localReadFeatures)
                           .prefer_gpu_device_type(vkb::PreferredDeviceType::discrete)
                           .select();
        if (!gpu_res) {
            CASPAR_THROW_EXCEPTION(caspar_exception()
                                   << msg_info("Failed to select physical device: " + gpu_res.error().message()));
        }
        _vkb_physical_device = gpu_res.value();

        CASPAR_LOG(info) << "Selected Vulkan device: " << _vkb_physical_device.properties.deviceName;

        vk::PhysicalDeviceRobustness2FeaturesEXT robustness2Features;
        robustness2Features.nullDescriptor = true;
        _vkb_physical_device.enable_extension_features_if_present(robustness2Features);

        for (auto& fn : requirements) {
            if (fn)
                fn(_vkb_physical_device);
        }

        // Snapshot the DMA-BUF import capability from what the requirements actually got
        // enabled — a module asks with enable_extension_if_present(), so "asked for" and
        // "got" are not the same thing on every GPU.
        {
            static const char* const dmabuf_extensions[] = {VK_KHR_EXTERNAL_MEMORY_FD_EXTENSION_NAME,
                                                            VK_EXT_EXTERNAL_MEMORY_DMA_BUF_EXTENSION_NAME,
                                                            VK_EXT_IMAGE_DRM_FORMAT_MODIFIER_EXTENSION_NAME,
                                                            VK_EXT_QUEUE_FAMILY_FOREIGN_EXTENSION_NAME};
            const auto enabled = _vkb_physical_device.get_extensions();
            dmabuf_import_     = std::all_of(std::begin(dmabuf_extensions), std::end(dmabuf_extensions), [&](auto* e) {
                return std::find(enabled.begin(), enabled.end(), std::string(e)) != enabled.end();
            });
            CASPAR_LOG(info) << L"vulkan: DMA-BUF import "
                             << (dmabuf_import_ ? L"available." : L"unavailable (extensions not enabled).");
        }

        // Same question for the sync_file bridge, but the extension being enabled is only
        // half of it: the driver must also report the SYNC_FD handle type as both importable
        // and exportable. NVIDIA in particular enables the extension while supporting only a
        // subset of handle types, so ask rather than assume.
        {
            const auto enabled = _vkb_physical_device.get_extensions();
            const bool has_ext = std::find(enabled.begin(),
                                           enabled.end(),
                                           std::string(VK_KHR_EXTERNAL_SEMAPHORE_FD_EXTENSION_NAME)) != enabled.end();
            if (has_ext) {
                vk::PhysicalDeviceExternalSemaphoreInfo info{};
                info.handleType = vk::ExternalSemaphoreHandleTypeFlagBits::eSyncFd;

                const auto props =
                    vk::PhysicalDevice(_vkb_physical_device.physical_device).getExternalSemaphoreProperties(info);

                const auto features = props.externalSemaphoreFeatures;
                sync_fd_semaphores_ = static_cast<bool>(features & vk::ExternalSemaphoreFeatureFlagBits::eImportable) &&
                                      static_cast<bool>(features & vk::ExternalSemaphoreFeatureFlagBits::eExportable);
            }
            CASPAR_LOG(info) << L"vulkan: sync_file semaphores "
                             << (sync_fd_semaphores_ ? L"available."
                                                     : L"unavailable (imported frames will block the CPU).");
        }

        // Create the logical device. The queue_manager scans the families and
        // resolves each kind of work (graphics/transfer/compute/video) to a
        // family; we feed its queue setup into the custom queue setup (queue count
        // is frozen at vkCreateDevice), then hand it the VkDevice so it can pull
        // the handles. The graphics queue is the primary render path; the rest go
        // to acquire(queue_type) clients (e.g. the screen consumer, hw decode).
        auto device_builder = vkb::DeviceBuilder(_vkb_physical_device);
        _physical_device    = vk::PhysicalDevice(_vkb_physical_device.physical_device);

        queue_manager_ = std::make_unique<queue_manager>(_physical_device);

        static const float                       queue_priority = 1.0f;
        std::vector<vkb::CustomQueueDescription> queue_descriptions;
        for (const auto& [family, count] : queue_manager_->queue_setup())
            queue_descriptions.emplace_back(family, std::vector<float>(count, queue_priority));
        device_builder.custom_queue_setup(queue_descriptions);

        auto device_res = device_builder.build();
        if (!device_res) {
            CASPAR_THROW_EXCEPTION(caspar_exception()
                                   << msg_info("Failed to create device: " + device_res.error().message()));
        }
        auto vkb_device = device_res.value();
        _device         = vk::Device(vkb_device.device);
        VULKAN_HPP_DEFAULT_DISPATCHER.init(_device);

        queue_manager_->create_queues(_device);

        VmaVulkanFunctions vulkanFunctions    = {};
        vulkanFunctions.vkGetInstanceProcAddr = _vkb_instance.fp_vkGetInstanceProcAddr;
        vulkanFunctions.vkGetDeviceProcAddr   = vkb_device.fp_vkGetDeviceProcAddr;

        VmaAllocatorCreateInfo allocatorCreateInfo = {};
        allocatorCreateInfo.flags                  = VMA_ALLOCATOR_CREATE_EXT_MEMORY_BUDGET_BIT;
        allocatorCreateInfo.vulkanApiVersion       = VK_API_VERSION_1_3;
        allocatorCreateInfo.physicalDevice         = _physical_device;
        allocatorCreateInfo.device                 = _device;
        allocatorCreateInfo.instance               = _vkb_instance.instance;
        allocatorCreateInfo.pVulkanFunctions       = &vulkanFunctions;

        vmaCreateAllocator(&allocatorCreateInfo, &_allocator);

        _memoryProperties = _physical_device.getMemoryProperties();
    }

    ~impl()
    {
        _device.waitIdle();

        for (auto& pool : host_pools_)
            pool.clear();

        for (auto& pools : device_pools_)
            for (auto& pool : pools)
                pool.clear();

        transfer_.reset();

        vmaDestroyAllocator(_allocator);

        _device.destroy();
        vkb::destroy_instance(_vkb_instance);
    }

    std::wstring version() { return version_; }

    uint32_t findDedicatedMemoryType(uint32_t typeMask, vk::MemoryPropertyFlags properties)
    {
        for (uint32_t i = 0; i < _memoryProperties.memoryTypeCount; ++i) {
            if ((typeMask & (1 << i)) &&
                ((_memoryProperties.memoryTypes[i].propertyFlags & properties) == properties)) {
                return i;
            }
        }
        throw std::runtime_error("Failed to find suitable memory type");
    }

    std::shared_ptr<texture> create_texture(int width, int height, int stride, common::bit_depth depth, bool clear)
    {
        CASPAR_VERIFY(stride > 0 && stride < 5);
        CASPAR_VERIFY(width > 0 && height > 0);

        static vk::Format INTERNAL_FORMAT[][5] = {{vk::Format::eUndefined,
                                                   vk::Format::eR8Unorm,
                                                   vk::Format::eR8G8Unorm,
                                                   vk::Format::eR8G8B8Unorm,
                                                   vk::Format::eR8G8B8A8Unorm},
                                                  {vk::Format::eUndefined,
                                                   vk::Format::eR16Unorm,
                                                   vk::Format::eR16G16Unorm,
                                                   vk::Format::eR16G16B16Unorm,
                                                   vk::Format::eR16G16B16A16Unorm}};

        auto depth_pool_index = depth == common::bit_depth::bit8 ? 0 : 1;
        auto format           = INTERNAL_FORMAT[depth_pool_index][stride];

        auto pool   = &device_pools_[depth_pool_index][stride - 1][(width << 16 & 0xFFFF0000) | (height & 0x0000FFFF)];
        auto extent = vk::Extent3D{static_cast<uint32_t>(width), static_cast<uint32_t>(height), 1};
        std::shared_ptr<texture> tex;
        if (!pool->try_pop(tex)) {
            vk::ImageCreateInfo imageInfo{};
            imageInfo.imageType     = vk::ImageType::e2D;
            imageInfo.format        = format;
            imageInfo.extent        = extent;
            imageInfo.mipLevels     = 1;
            imageInfo.arrayLayers   = 1;
            imageInfo.initialLayout = vk::ImageLayout::eUndefined;
            imageInfo.samples       = vk::SampleCountFlagBits::e1;
            imageInfo.tiling        = vk::ImageTiling::eOptimal;
            imageInfo.usage         = vk::ImageUsageFlagBits::eTransferDst | vk::ImageUsageFlagBits::eSampled;
            imageInfo.sharingMode   = vk::SharingMode::eExclusive;
            auto image              = _device.createImage(imageInfo);

            auto memReq = _device.getImageMemoryRequirements(image);

            vk::MemoryAllocateInfo allocInfo{};
            allocInfo.allocationSize = memReq.size;
            allocInfo.memoryTypeIndex =
                findDedicatedMemoryType(memReq.memoryTypeBits, vk::MemoryPropertyFlagBits::eDeviceLocal);

            auto imageMemory = _device.allocateMemory(allocInfo);
            _device.bindImageMemory(image, imageMemory, 0);
            auto clearValue = vk::ClearColorValue(std::array<float, 4>{0.0f, 0.0f, 0.0f, 1.0f});
            auto range      = vk::ImageSubresourceRange(vk::ImageAspectFlagBits::eColor, 0, 1, 0, 1);

            vk::ImageViewCreateInfo createInfo(
                {}, image, vk::ImageViewType::e2D, format, vk::ComponentMapping(), range);

            auto imageView = _device.createImageView(createInfo);

            tex = std::make_shared<texture>(width, height, stride, depth, image, imageMemory, imageView, _device);
        }
        tex->set_depth(depth);

        auto ptr = tex.get();
        return std::shared_ptr<texture>(
            ptr, [tex = std::move(tex), pool, self = shared_from_this()](texture*) mutable { pool->push(tex); });
    }

    std::shared_ptr<imported_image> import_dmabuf(const dmabuf_image& img)
    {
        if (!dmabuf_import_) {
            CASPAR_LOG(warning) << L"vulkan: DMA-BUF import requested but the extensions are not enabled.";
            return nullptr;
        }
        if (img.planes.empty() || img.planes.size() > 4 || img.width <= 0 || img.height <= 0 ||
            img.format == vk::Format::eUndefined) {
            CASPAR_LOG(warning) << L"vulkan: DMA-BUF import got a malformed descriptor.";
            return nullptr;
        }
        if (img.modifier == drm_format_mod_invalid) {
            // Refusing beats guessing linear: on a tiling-strict driver the guess reads the
            // memory with the wrong swizzle and produces a black or shredded picture rather
            // than an error the caller can fall back from.
            CASPAR_LOG(warning) << L"vulkan: DMA-BUF exporter did not report a DRM format modifier.";
            return nullptr;
        }
        // Every plane must live in the same buffer object: one bound VkDeviceMemory below.
        // Disjoint (fd-per-plane) images would need DISJOINT + one allocation per plane.
        for (const auto& p : img.planes) {
            if (p.fd < 0 || p.fd != img.planes.front().fd) {
                CASPAR_LOG(warning) << L"vulkan: DMA-BUF import only supports planes sharing one buffer.";
                return nullptr;
            }
        }

        const auto handle_type = vk::ExternalMemoryHandleTypeFlagBits::eDmaBufEXT;
        const auto usage       = vk::ImageUsageFlagBits::eTransferSrc;

        // Ask the driver whether it can import THIS format at THIS modifier before creating
        // anything, so an unsupported combination is a null return rather than a device-lost
        // some frames later.
        {
            vk::PhysicalDeviceExternalImageFormatInfo external_info{handle_type};
            vk::PhysicalDeviceImageDrmFormatModifierInfoEXT modifier_info{img.modifier, vk::SharingMode::eExclusive};
            modifier_info.pNext = &external_info;

            vk::PhysicalDeviceImageFormatInfo2 format_info{
                img.format, vk::ImageType::e2D, vk::ImageTiling::eDrmFormatModifierEXT, usage, {}};
            format_info.pNext = &modifier_info;

            vk::ExternalImageFormatProperties external_props;
            vk::ImageFormatProperties2        props;
            props.pNext = &external_props;

            const auto res = _physical_device.getImageFormatProperties2(&format_info, &props);
            if (res != vk::Result::eSuccess ||
                !(external_props.externalMemoryProperties.externalMemoryFeatures &
                  vk::ExternalMemoryFeatureFlagBits::eImportable)) {
                CASPAR_LOG(warning) << L"vulkan: driver cannot import DMA-BUF format " << static_cast<int>(img.format)
                                    << L" with modifier 0x" << std::hex << img.modifier << std::dec << L".";
                return nullptr;
            }
        }

        vk::Image        image;
        vk::DeviceMemory memory;
        int              dup_fd = -1;
        try {
            // The explicit plane layouts are how the driver learns where each plane starts
            // and how wide its rows are; together with the modifier they fully describe the
            // exporter's memory layout, which is what keeps the import zero-copy.
            std::vector<vk::SubresourceLayout> plane_layouts;
            plane_layouts.reserve(img.planes.size());
            for (const auto& p : img.planes) {
                vk::SubresourceLayout layout{};
                layout.offset     = p.offset;
                layout.rowPitch   = p.stride;
                layout.size       = 0; // required to be 0 here
                layout.arrayPitch = 0;
                layout.depthPitch = 0;
                plane_layouts.push_back(layout);
            }

            vk::ImageDrmFormatModifierExplicitCreateInfoEXT modifier_create{img.modifier, plane_layouts};
            vk::ExternalMemoryImageCreateInfo              external_create{handle_type};
            external_create.pNext = &modifier_create;

            vk::ImageCreateInfo image_info{};
            image_info.pNext         = &external_create;
            image_info.imageType     = vk::ImageType::e2D;
            image_info.format        = img.format;
            image_info.extent        = vk::Extent3D{static_cast<uint32_t>(img.width),
                                             static_cast<uint32_t>(img.height),
                                             1};
            image_info.mipLevels     = 1;
            image_info.arrayLayers   = 1;
            image_info.samples       = vk::SampleCountFlagBits::e1;
            image_info.tiling        = vk::ImageTiling::eDrmFormatModifierEXT;
            image_info.usage         = usage;
            image_info.sharingMode   = vk::SharingMode::eExclusive;
            image_info.initialLayout = vk::ImageLayout::eUndefined;

            image = _device.createImage(image_info);

            // The memory type must satisfy the image AND be one the fd can actually back.
            const auto memReq   = _device.getImageMemoryRequirements(image);
            const auto fd_props = _device.getMemoryFdPropertiesKHR(handle_type, img.planes.front().fd);

            const uint32_t type_bits = memReq.memoryTypeBits & fd_props.memoryTypeBits;
            if (type_bits == 0)
                CASPAR_THROW_EXCEPTION(caspar_exception()
                                       << msg_info("no memory type can back the imported DMA-BUF"));

            uint32_t type_index = 0;
            while (!(type_bits & (1u << type_index)))
                ++type_index;

            // Vulkan takes ownership of the fd it imports, so hand it a dup and leave the
            // caller's copy alone. On failure the dup is ours to close (see catch).
            dup_fd = dup_dmabuf_fd(img.planes.front().fd);
            if (dup_fd < 0)
                CASPAR_THROW_EXCEPTION(caspar_exception() << msg_info("dup() of the DMA-BUF fd failed"));

            // Dedicated: an imported image owns its whole allocation, and NVIDIA requires
            // the dedicated chain for external images.
            vk::MemoryDedicatedAllocateInfo dedicated{};
            dedicated.image = image;
            vk::ImportMemoryFdInfoKHR import_info{handle_type, dup_fd};
            import_info.pNext = &dedicated;

            vk::MemoryAllocateInfo alloc_info{memReq.size, type_index};
            alloc_info.pNext = &import_info;

            memory = _device.allocateMemory(alloc_info);
            dup_fd = -1; // consumed by the successful import; freeMemory closes it now
            _device.bindImageMemory(image, memory, 0);
        } catch (...) {
            if (dup_fd >= 0)
                close_dmabuf_fd(dup_fd);
            if (memory)
                _device.freeMemory(memory);
            if (image)
                _device.destroyImage(image);
            CASPAR_LOG_CURRENT_EXCEPTION();
            return nullptr;
        }

        return std::make_shared<imported_image>(
            _device, image, memory, img.width, img.height, img.format, img.modifier);
    }

    vk::Semaphore import_sync_fd_semaphore(int sync_fd)
    {
        if (!sync_fd_semaphores_ || sync_fd < 0)
            return nullptr;

        vk::Semaphore semaphore;
        try {
            semaphore = _device.createSemaphore(vk::SemaphoreCreateInfo{});

            vk::ImportSemaphoreFdInfoKHR import{};
            import.semaphore = semaphore;
            import.handleType = vk::ExternalSemaphoreHandleTypeFlagBits::eSyncFd;
            import.fd         = sync_fd;
            // SYNC_FD imports are required to be temporary: the payload lasts until a wait
            // consumes it, after which the semaphore reverts to its own (unsignalled) state.
            // That is what lets the same handle be re-imported next frame.
            import.flags = vk::SemaphoreImportFlagBits::eTemporary;

            _device.importSemaphoreFdKHR(import);
        } catch (const vk::SystemError& e) {
            // A refused import means the fd is not something this driver can wait on. The
            // caller falls back to blocking, so this is not fatal.
            CASPAR_LOG(warning) << L"vulkan: could not import a sync_file fence: " << u16(e.what());
            if (semaphore)
                _device.destroySemaphore(semaphore);
            return nullptr;
        }

        return semaphore;
    }

    vk::Semaphore create_exportable_semaphore()
    {
        if (!sync_fd_semaphores_)
            return nullptr;

        try {
            vk::ExportSemaphoreCreateInfo export_info{};
            export_info.handleTypes = vk::ExternalSemaphoreHandleTypeFlagBits::eSyncFd;

            vk::SemaphoreCreateInfo create{};
            create.pNext = &export_info;

            return _device.createSemaphore(create);
        } catch (const vk::SystemError& e) {
            CASPAR_LOG(warning) << L"vulkan: could not create an exportable semaphore: " << u16(e.what());
            return nullptr;
        }
    }

    int export_sync_fd(vk::Semaphore semaphore)
    {
        if (!sync_fd_semaphores_ || !semaphore)
            return -1;

        try {
            vk::SemaphoreGetFdInfoKHR get{};
            get.semaphore  = semaphore;
            get.handleType = vk::ExternalSemaphoreHandleTypeFlagBits::eSyncFd;

            // Exporting SYNC_FD also RESETS the semaphore's payload, which is why the caller
            // may hand the same semaphore back for the next frame.
            return _device.getSemaphoreFdKHR(get);
        } catch (const vk::SystemError& e) {
            CASPAR_LOG(warning) << L"vulkan: could not export a fence as a sync_file: " << u16(e.what());
            return -1;
        }
    }

    void destroy_semaphore(vk::Semaphore semaphore)
    {
        if (semaphore)
            _device.destroySemaphore(semaphore);
    }

    std::shared_ptr<buffer> create_buffer(int size, bool write)
    {
        CASPAR_VERIFY(size > 0);

        // TODO (perf) Shared pool.
        auto pool = &host_pools_[static_cast<int>(write ? 1 : 0)][size];

        std::shared_ptr<buffer> buf;
        if (!pool->try_pop(buf)) {
            buf = std::make_shared<buffer>(size, write, _allocator);
        }

        auto ptr = buf.get();
        return std::shared_ptr<buffer>(ptr, [buf = std::move(buf), self = shared_from_this()](buffer*) mutable {
            auto pool = &self->host_pools_[static_cast<int>(buf->write() ? 1 : 0)][buf->size()];
            pool->push(std::move(buf));
        });
    }

    array<uint8_t> create_array(int size)
    {
        auto buf = create_buffer(size, true);
        auto ptr = reinterpret_cast<uint8_t*>(buf->data());
        return array<uint8_t>(ptr, buf->size(), std::move(buf));
    }

    boost::property_tree::wptree info() const
    {
        boost::property_tree::wptree info;

        boost::property_tree::wptree pooled_device_buffers;
        size_t                       total_pooled_device_buffer_size  = 0;
        size_t                       total_pooled_device_buffer_count = 0;

        for (size_t i = 0; i < device_pools_.size(); ++i) {
            auto& depth_pools = device_pools_.at(i);
            for (size_t j = 0; j < depth_pools.size(); ++j) {
                auto& pools      = depth_pools.at(j);
                bool  mipmapping = j > 3;
                auto  stride     = mipmapping ? j - 3 : j + 1;

                for (auto& pool : pools) {
                    auto width  = pool.first >> 16;
                    auto height = pool.first & 0x0000FFFF;
                    auto size   = width * height * stride;
                    auto count  = pool.second.size();

                    if (count == 0)
                        continue;

                    boost::property_tree::wptree pool_info;

                    pool_info.add(L"stride", stride);
                    pool_info.add(L"mipmapping", mipmapping);
                    pool_info.add(L"width", width);
                    pool_info.add(L"height", height);
                    pool_info.add(L"size", size);
                    pool_info.add(L"count", count);

                    total_pooled_device_buffer_size += size * count;
                    total_pooled_device_buffer_count += count;

                    pooled_device_buffers.add_child(L"device_buffer_pool", pool_info);
                }
            }
        }

        info.add_child(L"gl.details.pooled_device_buffers", pooled_device_buffers);

        boost::property_tree::wptree pooled_host_buffers;
        size_t                       total_read_size   = 0;
        size_t                       total_write_size  = 0;
        size_t                       total_read_count  = 0;
        size_t                       total_write_count = 0;

        for (size_t i = 0; i < host_pools_.size(); ++i) {
            auto& pools    = host_pools_.at(i);
            auto  is_write = i == 1;

            for (auto& pool : pools) {
                auto size  = pool.first;
                auto count = pool.second.size();

                if (count == 0)
                    continue;

                boost::property_tree::wptree pool_info;

                pool_info.add(L"usage", is_write ? L"write_only" : L"read_only");
                pool_info.add(L"size", size);
                pool_info.add(L"count", count);

                pooled_host_buffers.add_child(L"host_buffer_pool", pool_info);

                (is_write ? total_write_count : total_read_count) += count;
                (is_write ? total_write_size : total_read_size) += size * count;
            }
        }

        info.add_child(L"gl.details.pooled_host_buffers", pooled_host_buffers);
        info.add(L"gl.summary.pooled_device_buffers.total_count", total_pooled_device_buffer_count);
        info.add(L"gl.summary.pooled_device_buffers.total_size", total_pooled_device_buffer_size);
        // info.add_child(L"gl.summary.all_device_buffers", texture::info());
        info.add(L"gl.summary.pooled_host_buffers.total_read_count", total_read_count);
        info.add(L"gl.summary.pooled_host_buffers.total_write_count", total_write_count);
        info.add(L"gl.summary.pooled_host_buffers.total_read_size", total_read_size);
        info.add(L"gl.summary.pooled_host_buffers.total_write_size", total_write_size);
        info.add_child(L"gl.summary.all_host_buffers", buffer::info());

        return info;
    }

    std::future<void> gc()
    {
        CASPAR_LOG(info) << " vulkan: Running GC.";

        try {
            for (auto& depth_pools : device_pools_) {
                for (auto& pools : depth_pools) {
                    for (auto& pool : pools)
                        pool.second.clear();
                }
            }
            for (auto& pools : host_pools_) {
                for (auto& pool : pools)
                    pool.second.clear();
            }
        } catch (...) {
            CASPAR_LOG_CURRENT_EXCEPTION();
        }

        return make_ready_future();
    }
};

device::device(const std::vector<vulkan_requirements_fn>& requirements)
    : impl_(new impl(requirements))
{
    // Created after impl_ is set so the transfer service can build its
    // command_context off this fully-constructed device's queue.
    impl_->transfer_ = std::make_unique<class transfer>(*this);
}
device::~device() {}

vk::PhysicalDeviceMemoryProperties device::getMemoryProperties() { return impl_->_memoryProperties; }
vk::Device                         device::getVkDevice() const { return impl_->_device; }
vk::Instance                       device::instance() const { return vk::Instance(impl_->_vkb_instance.instance); }
vk::PhysicalDevice                 device::physical_device() const { return impl_->_physical_device; }
std::shared_ptr<vulkan_queue>      device::queue() { return impl_->queue_manager_->primary(); }
std::shared_ptr<vulkan_queue> device::acquire_queue(queue_type type) { return impl_->queue_manager_->acquire(type); }
class transfer&               device::transfer() { return *impl_->transfer_; }

std::shared_ptr<texture> device::create_texture(int width, int height, int stride, common::bit_depth depth)
{
    return impl_->create_texture(width, height, stride, depth, true);
}
std::shared_ptr<buffer> device::create_buffer(int size, bool write) { return impl_->create_buffer(size, write); }
bool                    device::supports_dmabuf_import() const { return impl_->dmabuf_import_; }
std::shared_ptr<imported_image> device::import_dmabuf(const dmabuf_image& img) { return impl_->import_dmabuf(img); }
bool device::supports_sync_fd_semaphores() const { return impl_->sync_fd_semaphores_; }
vk::Semaphore device::import_sync_fd_semaphore(int sync_fd) { return impl_->import_sync_fd_semaphore(sync_fd); }
vk::Semaphore device::create_exportable_semaphore() { return impl_->create_exportable_semaphore(); }
int           device::export_sync_fd(vk::Semaphore semaphore) { return impl_->export_sync_fd(semaphore); }
void          device::destroy_semaphore(vk::Semaphore semaphore) { impl_->destroy_semaphore(semaphore); }
array<uint8_t>               device::create_array(int size) { return impl_->create_array(size); }
std::wstring                 device::version() const { return impl_->version(); }
boost::property_tree::wptree device::info() const { return impl_->info(); }
std::future<void>            device::gc() { return impl_->gc(); }
}}} // namespace caspar::accelerator::vulkan
