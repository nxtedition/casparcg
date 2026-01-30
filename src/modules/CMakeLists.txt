cmake_minimum_required (VERSION 3.16)
project("modules")

add_subdirectory(image)
add_subdirectory(ffmpeg)
add_subdirectory(oal)
add_subdirectory(decklink)
add_subdirectory(newtek)

# Artnet has boost::variant compatibility issues on macOS with current Boost version
if (NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")
	add_subdirectory(artnet)
endif()

# Screen consumer requires OpenGL, not yet available on macOS (Phase 9)
if (NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")
	add_subdirectory(screen)
endif()

if (ENABLE_HTML)
	add_subdirectory(html)
endif ()

if (MSVC)
	add_subdirectory(flash)
	add_subdirectory(bluefish)
endif()

