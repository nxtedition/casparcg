cmake_minimum_required (VERSION 3.16)
project (screen)

if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # macOS: Use Vulkan screen consumer (Phase 9)
    set(SOURCES
        consumer/screen_consumer_vk.cpp
        consumer/screen_consumer_vk.h

        screen.cpp
        screen.h
    )

    casparcg_add_module_project(screen
        SOURCES ${SOURCES}
        INIT_FUNCTION "screen::init"
    )
    target_include_directories(screen PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
        ${FFMPEG_INCLUDE_PATH}
    )
    target_link_libraries(screen PRIVATE accelerator ffmpeg glfw)
    target_link_libraries(screen PRIVATE Vulkan::Vulkan)
    target_link_libraries(screen PRIVATE Apple::Frameworks)
else()
    # Windows/Linux: Use OpenGL screen consumer
    set(SOURCES
        consumer/screen_consumer.cpp
        consumer/screen_consumer.h

        consumer_screen_vertex.h
        consumer_screen_fragment.h

        screen.cpp
        screen.h
    )

    bin2c("consumer/screen.vert" "consumer_screen_vertex.h" "caspar::screen" "vertex_shader")
    bin2c("consumer/screen.frag" "consumer_screen_fragment.h" "caspar::screen" "fragment_shader")

    if (MSVC)
    else ()
        list(APPEND SOURCES
            util/x11_util.cpp
            util/x11_util.h
        )
    endif ()

    casparcg_add_module_project(screen
        SOURCES ${SOURCES}
        INIT_FUNCTION "screen::init"
    )
    target_include_directories(screen PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}
        ${FFMPEG_INCLUDE_PATH}
    )
    target_link_libraries(screen PRIVATE accelerator ffmpeg GLEW::glew sfml-window)
endif()

set_target_properties(screen PROPERTIES FOLDER modules)
source_group(sources\\consumer consumer/*)
source_group(sources ./*)
