/*
 * Copyright 2013 Sveriges Television AB http://casparcg.com/
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
 * Author: Julian Waller, julian@supergly.tv
 */

#pragma once

#include <cstdlib>
#include <functional>
#include <future>
#include <string>

namespace caspar::html {

#if defined(__unix__) && !defined(__APPLE__)
// Which display server this process can reach. On Linux this decides Chromium's ozone platform,
// and with it whether a DMA-BUF can be allocated at all: ozone's headless platform cannot —
// HeadlessSurfaceFactory::CreateNativePixmap returns a stub — so shared-texture OSR needs a real
// x11 or wayland platform behind it.
enum class display_server
{
    none,
    x11,
    wayland,
};

inline display_server detect_display_server()
{
    // X11 first, and deliberately so even when both are set: CEF's own configuration for
    // shared-texture OSR pins ozone-platform=x11 regardless of the session type
    // (cef/tests/shared/browser/client_app_browser.cc).
    if (const char* display = std::getenv("DISPLAY"); display != nullptr && *display != '\0')
        return display_server::x11;
    if (const char* wayland = std::getenv("WAYLAND_DISPLAY"); wayland != nullptr && *wayland != '\0')
        return display_server::wayland;
    return display_server::none;
}
#endif

const std::string REMOVE_MESSAGE_NAME = "CasparCGRemove";
const std::string LOG_MESSAGE_NAME    = "CasparCGLog";

void              invoke(const std::function<void()>& func);
std::future<void> begin_invoke(const std::function<void()>& func);

} // namespace caspar::html
