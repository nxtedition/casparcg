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

#include "../StdAfx.h"

#include "pipeline.h"
#include "texture.h"
#include "vk_check.h"

#include <common/log.h>

#include <vulkan/vulkan.h>

// Include the compiled SPIR-V shader
#include <vk_blend_shader.h>

namespace caspar { namespace accelerator { namespace vk {

struct blend_pipeline::impl
{
    VkDevice              device_              = VK_NULL_HANDLE;
    VkPhysicalDevice      physical_device_     = VK_NULL_HANDLE;
    VkCommandPool         command_pool_        = VK_NULL_HANDLE;
    VkQueue               queue_               = VK_NULL_HANDLE;
    VkShaderModule        shader_module_       = VK_NULL_HANDLE;
    VkDescriptorSetLayout descriptor_layout_   = VK_NULL_HANDLE;
    VkPipelineLayout      pipeline_layout_     = VK_NULL_HANDLE;
    VkPipeline            pipeline_            = VK_NULL_HANDLE;
    VkDescriptorPool      descriptor_pool_     = VK_NULL_HANDLE;

    impl(void* device, void* physical_device, void* command_pool, void* queue)
        : device_(static_cast<VkDevice>(device))
        , physical_device_(static_cast<VkPhysicalDevice>(physical_device))
        , command_pool_(static_cast<VkCommandPool>(command_pool))
        , queue_(static_cast<VkQueue>(queue))
    {
        create_shader_module();
        create_descriptor_layout();
        create_pipeline_layout();
        create_pipeline();
        create_descriptor_pool();

        CASPAR_LOG(info) << L"[vk::blend_pipeline] Vulkan blend compute pipeline initialized (Phase 7 - multi-plane)";
    }

    ~impl()
    {
        if (device_ != VK_NULL_HANDLE) {
            vkDeviceWaitIdle(device_);

            if (descriptor_pool_ != VK_NULL_HANDLE) {
                vkDestroyDescriptorPool(device_, descriptor_pool_, nullptr);
            }
            if (pipeline_ != VK_NULL_HANDLE) {
                vkDestroyPipeline(device_, pipeline_, nullptr);
            }
            if (pipeline_layout_ != VK_NULL_HANDLE) {
                vkDestroyPipelineLayout(device_, pipeline_layout_, nullptr);
            }
            if (descriptor_layout_ != VK_NULL_HANDLE) {
                vkDestroyDescriptorSetLayout(device_, descriptor_layout_, nullptr);
            }
            if (shader_module_ != VK_NULL_HANDLE) {
                vkDestroyShaderModule(device_, shader_module_, nullptr);
            }
        }
    }

    void create_shader_module()
    {
        VkShaderModuleCreateInfo createInfo{};
        createInfo.sType    = VK_STRUCTURE_TYPE_SHADER_MODULE_CREATE_INFO;
        createInfo.codeSize = blend_shader_spv_size * sizeof(uint32_t);
        createInfo.pCode    = blend_shader_spv;

        VK(vkCreateShaderModule(device_, &createInfo, nullptr, &shader_module_));
    }

    void create_descriptor_layout()
    {
        // Phase 7: 5 storage images: 4 source planes (read-only) + 1 dst (read-write)
        std::array<VkDescriptorSetLayoutBinding, 5> bindings{};

        // Binding 0-3: source planes (read-only)
        for (int i = 0; i < 4; ++i) {
            bindings[i].binding            = i;
            bindings[i].descriptorType     = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
            bindings[i].descriptorCount    = 1;
            bindings[i].stageFlags         = VK_SHADER_STAGE_COMPUTE_BIT;
            bindings[i].pImmutableSamplers = nullptr;
        }

        // Binding 4: destination image (read-write)
        bindings[4].binding            = 4;
        bindings[4].descriptorType     = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
        bindings[4].descriptorCount    = 1;
        bindings[4].stageFlags         = VK_SHADER_STAGE_COMPUTE_BIT;
        bindings[4].pImmutableSamplers = nullptr;

        VkDescriptorSetLayoutCreateInfo layoutInfo{};
        layoutInfo.sType        = VK_STRUCTURE_TYPE_DESCRIPTOR_SET_LAYOUT_CREATE_INFO;
        layoutInfo.bindingCount = static_cast<uint32_t>(bindings.size());
        layoutInfo.pBindings    = bindings.data();

        VK(vkCreateDescriptorSetLayout(device_, &layoutInfo, nullptr, &descriptor_layout_));
    }

    void create_pipeline_layout()
    {
        VkPushConstantRange pushConstantRange{};
        pushConstantRange.stageFlags = VK_SHADER_STAGE_COMPUTE_BIT;
        pushConstantRange.offset     = 0;
        pushConstantRange.size       = sizeof(blend_push_constants);

        VkPipelineLayoutCreateInfo pipelineLayoutInfo{};
        pipelineLayoutInfo.sType                  = VK_STRUCTURE_TYPE_PIPELINE_LAYOUT_CREATE_INFO;
        pipelineLayoutInfo.setLayoutCount         = 1;
        pipelineLayoutInfo.pSetLayouts            = &descriptor_layout_;
        pipelineLayoutInfo.pushConstantRangeCount = 1;
        pipelineLayoutInfo.pPushConstantRanges    = &pushConstantRange;

        VK(vkCreatePipelineLayout(device_, &pipelineLayoutInfo, nullptr, &pipeline_layout_));
    }

    void create_pipeline()
    {
        VkPipelineShaderStageCreateInfo shaderStageInfo{};
        shaderStageInfo.sType  = VK_STRUCTURE_TYPE_PIPELINE_SHADER_STAGE_CREATE_INFO;
        shaderStageInfo.stage  = VK_SHADER_STAGE_COMPUTE_BIT;
        shaderStageInfo.module = shader_module_;
        shaderStageInfo.pName  = "main";

        VkComputePipelineCreateInfo pipelineInfo{};
        pipelineInfo.sType  = VK_STRUCTURE_TYPE_COMPUTE_PIPELINE_CREATE_INFO;
        pipelineInfo.stage  = shaderStageInfo;
        pipelineInfo.layout = pipeline_layout_;

        VK(vkCreateComputePipelines(device_, VK_NULL_HANDLE, 1, &pipelineInfo, nullptr, &pipeline_));
    }

    void create_descriptor_pool()
    {
        VkDescriptorPoolSize poolSize{};
        poolSize.type            = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
        poolSize.descriptorCount = 5;  // Phase 7: 4 source planes + 1 dst image

        VkDescriptorPoolCreateInfo poolInfo{};
        poolInfo.sType         = VK_STRUCTURE_TYPE_DESCRIPTOR_POOL_CREATE_INFO;
        poolInfo.poolSizeCount = 1;
        poolInfo.pPoolSizes    = &poolSize;
        poolInfo.maxSets       = 1;
        poolInfo.flags         = VK_DESCRIPTOR_POOL_CREATE_FREE_DESCRIPTOR_SET_BIT;

        VK(vkCreateDescriptorPool(device_, &poolInfo, nullptr, &descriptor_pool_));
    }

    VkCommandBuffer begin_command_buffer()
    {
        VkCommandBufferAllocateInfo allocInfo{};
        allocInfo.sType              = VK_STRUCTURE_TYPE_COMMAND_BUFFER_ALLOCATE_INFO;
        allocInfo.commandPool        = command_pool_;
        allocInfo.level              = VK_COMMAND_BUFFER_LEVEL_PRIMARY;
        allocInfo.commandBufferCount = 1;

        VkCommandBuffer cmdBuffer;
        VK(vkAllocateCommandBuffers(device_, &allocInfo, &cmdBuffer));

        VkCommandBufferBeginInfo beginInfo{};
        beginInfo.sType = VK_STRUCTURE_TYPE_COMMAND_BUFFER_BEGIN_INFO;
        beginInfo.flags = VK_COMMAND_BUFFER_USAGE_ONE_TIME_SUBMIT_BIT;

        VK(vkBeginCommandBuffer(cmdBuffer, &beginInfo));

        return cmdBuffer;
    }

    void end_command_buffer(VkCommandBuffer cmdBuffer)
    {
        VK(vkEndCommandBuffer(cmdBuffer));

        VkSubmitInfo submitInfo{};
        submitInfo.sType              = VK_STRUCTURE_TYPE_SUBMIT_INFO;
        submitInfo.commandBufferCount = 1;
        submitInfo.pCommandBuffers    = &cmdBuffer;

        VK(vkQueueSubmit(queue_, 1, &submitInfo, VK_NULL_HANDLE));
        VK(vkQueueWaitIdle(queue_));

        vkFreeCommandBuffers(device_, command_pool_, 1, &cmdBuffer);
    }

    void execute(texture& src, texture& dst, const blend_push_constants& params)
    {
        // Phase 7: Use multi-plane execute with single source texture bound to all planes
        execute_internal(&src, nullptr, nullptr, nullptr, dst, params);
    }

    void execute(const std::vector<std::shared_ptr<texture>>& planes, texture& dst, const blend_push_constants& params)
    {
        // Phase 7: Multi-plane texture support
        texture* plane_ptrs[4] = {nullptr, nullptr, nullptr, nullptr};
        for (size_t i = 0; i < planes.size() && i < 4; ++i) {
            if (planes[i]) {
                plane_ptrs[i] = planes[i].get();
            }
        }
        execute_internal(plane_ptrs[0], plane_ptrs[1], plane_ptrs[2], plane_ptrs[3], dst, params);
    }

    void execute_internal(texture* plane0, texture* plane1, texture* plane2, texture* plane3,
                          texture& dst, const blend_push_constants& params)
    {
        // Allocate descriptor set
        VkDescriptorSetAllocateInfo allocInfo{};
        allocInfo.sType              = VK_STRUCTURE_TYPE_DESCRIPTOR_SET_ALLOCATE_INFO;
        allocInfo.descriptorPool     = descriptor_pool_;
        allocInfo.descriptorSetCount = 1;
        allocInfo.pSetLayouts        = &descriptor_layout_;

        VkDescriptorSet descriptorSet;
        VK(vkAllocateDescriptorSets(device_, &allocInfo, &descriptorSet));

        // Transition textures to GENERAL layout for compute access
        if (plane0) plane0->transition_to_general();
        if (plane1) plane1->transition_to_general();
        if (plane2) plane2->transition_to_general();
        if (plane3) plane3->transition_to_general();
        dst.transition_to_general();

        // Phase 7: Set up image info for all 5 bindings (4 planes + 1 dst)
        std::array<VkDescriptorImageInfo, 5> imageInfos{};

        // Use plane0 as fallback for unused planes (shader will ignore based on pixel_format)
        VkImageView fallback_view = plane0 ? static_cast<VkImageView>(plane0->image_view()) : VK_NULL_HANDLE;

        imageInfos[0].imageView   = plane0 ? static_cast<VkImageView>(plane0->image_view()) : fallback_view;
        imageInfos[0].imageLayout = VK_IMAGE_LAYOUT_GENERAL;

        imageInfos[1].imageView   = plane1 ? static_cast<VkImageView>(plane1->image_view()) : fallback_view;
        imageInfos[1].imageLayout = VK_IMAGE_LAYOUT_GENERAL;

        imageInfos[2].imageView   = plane2 ? static_cast<VkImageView>(plane2->image_view()) : fallback_view;
        imageInfos[2].imageLayout = VK_IMAGE_LAYOUT_GENERAL;

        imageInfos[3].imageView   = plane3 ? static_cast<VkImageView>(plane3->image_view()) : fallback_view;
        imageInfos[3].imageLayout = VK_IMAGE_LAYOUT_GENERAL;

        imageInfos[4].imageView   = static_cast<VkImageView>(dst.image_view());
        imageInfos[4].imageLayout = VK_IMAGE_LAYOUT_GENERAL;

        std::array<VkWriteDescriptorSet, 5> descriptorWrites{};
        for (int i = 0; i < 5; ++i) {
            descriptorWrites[i].sType           = VK_STRUCTURE_TYPE_WRITE_DESCRIPTOR_SET;
            descriptorWrites[i].dstSet          = descriptorSet;
            descriptorWrites[i].dstBinding      = i;
            descriptorWrites[i].dstArrayElement = 0;
            descriptorWrites[i].descriptorType  = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
            descriptorWrites[i].descriptorCount = 1;
            descriptorWrites[i].pImageInfo      = &imageInfos[i];
        }

        vkUpdateDescriptorSets(device_, static_cast<uint32_t>(descriptorWrites.size()), descriptorWrites.data(), 0, nullptr);

        // Record command buffer
        auto cmdBuffer = begin_command_buffer();

        // Bind pipeline and descriptor set
        vkCmdBindPipeline(cmdBuffer, VK_PIPELINE_BIND_POINT_COMPUTE, pipeline_);
        vkCmdBindDescriptorSets(cmdBuffer, VK_PIPELINE_BIND_POINT_COMPUTE, pipeline_layout_, 0, 1, &descriptorSet, 0, nullptr);

        // Push constants
        vkCmdPushConstants(cmdBuffer, pipeline_layout_, VK_SHADER_STAGE_COMPUTE_BIT, 0, sizeof(blend_push_constants), &params);

        // Dispatch compute shader
        // Work group size is 16x16, so we need to dispatch enough groups to cover the destination
        uint32_t groupCountX = (params.dst_width + 15) / 16;
        uint32_t groupCountY = (params.dst_height + 15) / 16;
        vkCmdDispatch(cmdBuffer, groupCountX, groupCountY, 1);

        end_command_buffer(cmdBuffer);

        // Free descriptor set
        vkFreeDescriptorSets(device_, descriptor_pool_, 1, &descriptorSet);

        // Transition destination back to shader read optimal
        dst.transition_to_shader_read();
    }
};

blend_pipeline::blend_pipeline(void* device, void* physical_device, void* command_pool, void* queue)
    : impl_(std::make_unique<impl>(device, physical_device, command_pool, queue))
{
}

blend_pipeline::~blend_pipeline() = default;

void blend_pipeline::execute(texture& src, texture& dst, const blend_push_constants& params)
{
    impl_->execute(src, dst, params);
}

void blend_pipeline::execute(const std::vector<std::shared_ptr<texture>>& planes, texture& dst, const blend_push_constants& params)
{
    impl_->execute(planes, dst, params);
}

}}} // namespace caspar::accelerator::vk
