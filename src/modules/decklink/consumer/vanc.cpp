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

#include "vanc.h"
#include <boost/lexical_cast.hpp>

namespace caspar { namespace decklink {

class decklink_scte104_packet : public IDeckLinkAncillaryPacket
{
    std::atomic<int> ref_count_{0};

    unsigned int line_number_{0};

    std::shared_ptr<klvanc_packet_scte_104_s> pkt_;
    std::shared_ptr<void>                     data_;
    std::shared_ptr<klvanc_context_s>         ctx_;

  public:
    decklink_scte104_packet(std::shared_ptr<klvanc_context_s>         ctx,
                            std::shared_ptr<klvanc_packet_scte_104_s> pkt,
                            unsigned int                              line_number = 9)
        : line_number_(line_number)
        , pkt_(pkt)
        , ctx_(ctx)
    {
    }

    // IUnknown

    HRESULT STDMETHODCALLTYPE QueryInterface(REFIID, LPVOID*) override { return E_NOINTERFACE; }

    ULONG STDMETHODCALLTYPE AddRef() override { return ++ref_count_; }

    ULONG STDMETHODCALLTYPE Release() override
    {
        if (--ref_count_ == 0) {
            delete this;

            return 0;
        }

        return ref_count_;
    }

    // IDeckLinkAncillaryPacket

    HRESULT STDMETHODCALLTYPE GetBytes(BMDAncillaryPacketFormat format, const void** data, unsigned int* size) override
    {
        if (format == bmdAncillaryPacketFormatUInt8) {
            uint8_t* bytes;
            uint16_t bytesCount;

            klvanc_convert_SCTE_104_to_packetBytes(ctx_.get(),
                                                   pkt_.get(),
                                                   &bytes,
                                                   &bytesCount); // TODO: Check for failure.

            void* ptr = reinterpret_cast<void*>(bytes);

            *data = data;
            *size = bytesCount;

            data_.reset(ptr, free);
        } else {
            return E_NOTIMPL;
        }

        return S_OK;
    }

    unsigned char STDMETHODCALLTYPE GetDID(void) override { return 0x41; }

    unsigned char STDMETHODCALLTYPE GetSDID(void) override { return 0x07; }

    unsigned int STDMETHODCALLTYPE GetLineNumber(void) override { return line_number_; }

    unsigned char STDMETHODCALLTYPE GetDataStreamIndex(void) override { return 0; }
};

class decklink_op47_packet : public IDeckLinkAncillaryPacket
{
    std::atomic<int> ref_count_{0};

    unsigned int line_number_;

    std::shared_ptr<void>             data_;
    std::shared_ptr<klvanc_context_s> ctx_;

    struct wst_packet
    {
        uint8_t  clock_run_in[2]{0x55, 0x55};
        uint8_t  framing_code{0x27};
        uint16_t payload[42]; // mrag address + 40 bytes of data
    };

  public:
    decklink_op47_packet(std::shared_ptr<klvanc_context_s> ctx, unsigned int line_number = 21)
        : line_number_(line_number)
        , ctx_(ctx)
    {
    }

    // IUnknown

    HRESULT STDMETHODCALLTYPE QueryInterface(REFIID, LPVOID*) override { return E_NOINTERFACE; }

    ULONG STDMETHODCALLTYPE AddRef() override { return ++ref_count_; }

    ULONG STDMETHODCALLTYPE Release() override
    {
        if (--ref_count_ == 0) {
            delete this;

            return 0;
        }

        return ref_count_;
    }

    // IDeckLinkAncillaryPacket
    HRESULT STDMETHODCALLTYPE GetBytes(BMDAncillaryPacketFormat format, const void** data, unsigned int* size) override
    {
        if (format == bmdAncillaryPacketFormatUInt8) {
            return E_NOTIMPL;
        } else if (format == bmdAncillaryPacketFormatUInt16) {
            return E_NOTIMPL;
        } else {
            return E_NOTIMPL;
        }

        return S_OK;
    }

    unsigned char STDMETHODCALLTYPE GetDID(void) override { return 0x43; }

    unsigned char STDMETHODCALLTYPE GetSDID(void) override { return 0x02; }

    unsigned int STDMETHODCALLTYPE GetLineNumber(void) override { return line_number_; }

    unsigned char STDMETHODCALLTYPE GetDataStreamIndex(void) override { return 0; }
};

decklink_vanc::decklink_vanc()
{
    {
        klvanc_context_s* vanchdl;
        klvanc_context_create(&vanchdl); // TODO: Check for failure.
        ctx_.reset(vanchdl, klvanc_context_destroy);
    }
}

std::vector<caspar::decklink::com_ptr<IDeckLinkAncillaryPacket>> decklink_vanc::create_vanc_packets()
{
    std::vector<caspar::decklink::com_ptr<IDeckLinkAncillaryPacket>> packets;
    if (scte_104_pkt_) {
        auto packet = wrap_raw<com_ptr, IDeckLinkAncillaryPacket>(new decklink_scte104_packet(ctx_, scte_104_pkt_));
        packets.push_back(packet);
        scte_104_pkt_.reset();
    }

    return packets;
}

bool decklink_vanc::create_op47_package(const std::vector<std::wstring>& params) { return false; }

bool decklink_vanc::create_scte104_package(const std::vector<std::wstring>& params)
{
    auto idx = 1;

    std::lock_guard<std::mutex> lock(mutex_);

    std::shared_ptr<klvanc_packet_scte_104_s> new_packet;
    {
        klvanc_packet_scte_104_s* pkt;
        if (FAILED(klvanc_alloc_SCTE_104(0xffff, &pkt))) {
            CASPAR_LOG(error) << "Failed to allocate SCTE 104 packet.";
            return false;
        }
        new_packet.reset(pkt, klvanc_free_SCTE_104);
    }

    try {
        if (boost::iequals(params.at(idx++), L"SPLICE_REQUEST_DATA")) {
            klvanc_multiple_operation_message_operation* op = nullptr;

            if (FAILED(klvanc_SCTE_104_Add_MOM_Op(new_packet.get(), MO_SPLICE_REQUEST_DATA, &op))) {
                CASPAR_LOG(error) << "Failed to add splice request data operation.";
                return false;
            }

            while (idx < params.size()) {
                auto key = params.at(idx++);
                auto val = params.at(idx++);

                if (boost::iequals(key, L"SPLICE_INSERT_TYPE")) {
                    op->sr_data.splice_insert_type = boost::lexical_cast<unsigned int>(val);
                } else if (boost::iequals(key, L"SPLICE_EVENT_ID")) {
                    op->sr_data.splice_event_id = boost::lexical_cast<unsigned int>(val);
                } else if (boost::iequals(key, L"PRE_ROLL_TIME")) {
                    op->sr_data.pre_roll_time = boost::lexical_cast<unsigned short>(val);
                } else if (boost::iequals(key, L"BREAK_DURATION")) {
                    op->sr_data.brk_duration = boost::lexical_cast<unsigned short>(val);
                } else if (boost::iequals(key, L"AVAIL_NUM")) {
                    op->sr_data.avail_num = static_cast<unsigned char>(boost::lexical_cast<unsigned short>(val));
                } else if (boost::iequals(key, L"AVAILS_EXPECTED")) {
                    op->sr_data.avails_expected = static_cast<unsigned char>(boost::lexical_cast<unsigned short>(val));
                } else if (boost::iequals(key, L"AUTO_RETURN_FLAG")) {
                    op->sr_data.auto_return_flag = static_cast<unsigned char>(boost::lexical_cast<unsigned short>(val));
                }
            }
            scte_104_pkt_ = new_packet;
        }
    } catch (const boost::bad_lexical_cast& e) {
        CASPAR_LOG(error) << "Failed to parse SCTE 104 parameters: " << e.what();
        return false;
    } catch (const std::out_of_range& e) {
        CASPAR_LOG(error) << "Failed to parse SCTE 104 parameters: Invalid number of arguments";
        return false;
    }

    return true;
}

std::shared_ptr<decklink_vanc> create_vanc() { return std::make_shared<decklink_vanc>(); }

}} // namespace caspar::decklink
