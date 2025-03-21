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
 * Author: Niklas Andersson, niklas.andersson@nxtedition.com
 */

#pragma once
#include "../decklink_api.h"
#include <common/memory.h>
#include <libklvanc/vanc-lines.h>
#include <libklvanc/vanc-scte_104.h>
#include <libklvanc/vanc.h>
#include <mutex>

namespace caspar { namespace decklink {
class decklink_vanc
{
    std::shared_ptr<klvanc_context_s>         ctx_;
    std::mutex                                mutex_;
    std::shared_ptr<klvanc_packet_scte_104_s> scte_104_pkt_;

  public:
    decklink_vanc();
    bool has_data() const { return scte_104_pkt_.get() != nullptr; }
    std::vector<caspar::decklink::com_ptr<IDeckLinkAncillaryPacket>> create_vanc_packets();
    bool create_scte104_package(const std::vector<std::wstring>& params);
    bool create_op47_package(const std::vector<std::wstring>& params);
};

std::shared_ptr<decklink_vanc> create_vanc();
}} // namespace caspar::decklink