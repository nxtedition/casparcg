#include "../util/base64.hpp"
#include "vanc.h"
#include <boost/lexical_cast.hpp>
#include <libklvanc/vanc-lines.h>
#include <libklvanc/vanc-scte_104.h>
#include <libklvanc/vanc.h>

namespace caspar { namespace decklink {

const uint8_t SCTE104_DID  = 0x41;
const uint8_t SCTE104_SDID = 0x07;

class vanc_scte104_strategy : public decklink_vanc_strategy
{
    static const std::wstring Name;

    std::shared_ptr<klvanc_context_s>         ctx_;
    std::mutex                                mutex_;
    std::shared_ptr<klvanc_packet_scte_104_s> scte_104_pkt_;
    uint8_t                                   line_number_;

    std::vector<uint8_t> payload_ = {};

  public:
    explicit vanc_scte104_strategy(uint8_t line_number)
        : line_number_(line_number)
    {
        klvanc_context_s* vanchdl;
        klvanc_context_create(&vanchdl); // TODO: Check for failure.
        ctx_.reset(vanchdl, klvanc_context_destroy);
    }

    virtual bool        has_data() const { return scte_104_pkt_.get() != nullptr || !payload_.empty(); }
    virtual vanc_packet pop_packet()
    {
        // If we have a payload, return it as a vanc_packet.
        if (payload_.size() > 0) {
            vanc_packet pkt{SCTE104_DID, SCTE104_SDID, line_number_, payload_};
            payload_.clear();
            return pkt;
        }

        // If we have a SCTE-104 packet, convert it to bytes and return as vanc_packet.
        uint8_t* bytes;
        uint16_t bytesCount;

        klvanc_convert_SCTE_104_to_packetBytes(ctx_.get(),
                                               scte_104_pkt_.get(),
                                               &bytes,
                                               &bytesCount); // TODO: Check for failure.

        void*                 ptr = reinterpret_cast<void*>(bytes);
        std::shared_ptr<void> data(ptr, free);

        scte_104_pkt_.reset();

        std::vector<uint8_t> data_vector(bytes, bytes + bytesCount);
        return {SCTE104_DID, SCTE104_SDID, line_number_, data_vector};
    }

    virtual bool try_push_data(const std::vector<std::wstring>& params)
    {
        auto idx = 1;

        std::lock_guard<std::mutex> lock(mutex_);

        try {
            if (boost::iequals(params.at(idx++), L"SPLICE_REQUEST_DATA")) {
                std::shared_ptr<klvanc_packet_scte_104_s> new_packet;
                {
                    klvanc_packet_scte_104_s* pkt;
                    if (FAILED(klvanc_alloc_SCTE_104(0xffff, &pkt))) {
                        CASPAR_LOG(error) << "Failed to allocate SCTE 104 packet.";
                        return false;
                    }
                    new_packet.reset(pkt, klvanc_free_SCTE_104);
                }

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
                        op->sr_data.avails_expected =
                            static_cast<unsigned char>(boost::lexical_cast<unsigned short>(val));
                    } else if (boost::iequals(key, L"AUTO_RETURN_FLAG")) {
                        op->sr_data.auto_return_flag =
                            static_cast<unsigned char>(boost::lexical_cast<unsigned short>(val));
                    }
                }
                scte_104_pkt_ = new_packet;
            } else if (params.size() == 2) {
                // try to parse the payload as a base64 encoded raw SCTE-104 packet.
                auto base64_payload = params.at(1);
                payload_            = base64_decode(base64_payload);
            }
        } catch (const boost::bad_lexical_cast& e) {
            CASPAR_LOG(error) << "Failed to parse SCTE 104 parameters: " << e.what();
            return false;
        } catch (const std::out_of_range& e) {
            CASPAR_LOG(error) << "Failed to parse SCTE 104 parameters: Invalid number of arguments";
            return false;
        } catch (const std::exception& e) {
            CASPAR_LOG(error) << "Failed to parse SCTE 104 parameters: " << e.what();
            return false;
        }

        return true;
    }

    virtual const std::wstring& get_name() const { return vanc_scte104_strategy::Name; }

  private:
    std::vector<uint8_t> base64_decode(const std::wstring& encoded)
    {
        std::vector<char> buffer(encoded.size());
        std::use_facet<std::ctype<wchar_t>>(std::locale())
            .narrow(encoded.data(), encoded.data() + encoded.size(), '?', buffer.data());
        auto str = std::string(buffer.data(), buffer.size());
        return base64::decode_into<std::vector<uint8_t>>(str);
    }
};

const std::wstring vanc_scte104_strategy::Name = L"SCTE104";

std::shared_ptr<decklink_vanc_strategy> create_scte104_strategy(uint8_t line_number)
{
    return std::make_shared<vanc_scte104_strategy>(line_number);
}

}} // namespace caspar::decklink