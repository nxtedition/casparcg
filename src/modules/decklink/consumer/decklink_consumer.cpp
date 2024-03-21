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
 * Author: Robert Nagy, ronag89@gmail.com
 */

#include "decklink_consumer.h"

#include "../decklink.h"
#include "../util/util.h"

#include "../decklink_api.h"

#include <core/consumer/frame_consumer.h>
#include <core/diagnostics/call_context.h>
#include <core/frame/frame.h>
#include <core/mixer/audio/audio_mixer.h>

#include <common/array.h>
#include <common/diagnostics/graph.h>
#include <common/except.h>
#include <common/executor.h>
#include <common/future.h>
#include <common/memshfl.h>
#include <common/param.h>
#include <common/timer.h>

#include <cstdlib>

#include <boost/circular_buffer.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>

extern "C" {
#include <libswscale/swscale.h>
}

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <thread>

namespace caspar { namespace decklink {

struct configuration
{
    enum class keyer_t
    {
        internal_keyer,
        external_keyer,
        default_keyer = external_keyer
    };

    enum class latency_t
    {
        low_latency,
        normal_latency,
        default_latency = normal_latency
    };

    int       device_index      = 1;
    int       key_device_idx    = 0;
    bool      embedded_audio    = false;
    keyer_t   keyer             = keyer_t::default_keyer;
    latency_t latency           = latency_t::default_latency;
    bool      key_only          = false;
    int       base_buffer_depth = 3;
    bool      hdr               = false;

    int buffer_depth() const
    {
        return base_buffer_depth + (latency == latency_t::low_latency ? 0 : 1) +
               (embedded_audio ? 1 : 0); // TODO: Do we need this?
    }

    int key_device_index() const { return key_device_idx == 0 ? device_index + 1 : key_device_idx; }
};

template <typename Configuration>
void set_latency(const com_iface_ptr<Configuration>& config,
                 configuration::latency_t            latency,
                 const std::wstring&                 print)
{
    if (latency == configuration::latency_t::low_latency) {
        config->SetFlag(bmdDeckLinkConfigLowLatencyVideoOutput, true);
        CASPAR_LOG(info) << print << L" Enabled low-latency mode.";
    } else if (latency == configuration::latency_t::normal_latency) {
        config->SetFlag(bmdDeckLinkConfigLowLatencyVideoOutput, false);
        CASPAR_LOG(info) << print << L" Disabled low-latency mode.";
    }
}

BMDPixelFormat get_pixel_format(bool hdr) { return hdr ? bmdFormat10BitRGBX : bmdFormat8BitBGRA; }
int            get_row_bytes(const core::video_format_desc& format_desc, bool hdr)
{
    return hdr ? ((format_desc.width + 63) / 64) * 256 : format_desc.width * 4;
}

com_ptr<IDeckLinkDisplayMode> get_display_mode(const com_iface_ptr<IDeckLinkOutput>& device,
                                               core::video_format                    fmt,
                                               BMDPixelFormat                        pix_fmt,
                                               BMDSupportedVideoModeFlags            flag,
                                               bool                                  hdr)
{
    auto format = get_decklink_video_format(fmt);

    IDeckLinkDisplayMode*         m = nullptr;
    IDeckLinkDisplayModeIterator* iter;

    if (SUCCEEDED(device->GetDisplayModeIterator(&iter))) {
        auto iterator = wrap_raw<com_ptr>(iter, true);
        while (SUCCEEDED(iterator->Next(&m)) && m != nullptr && m->GetDisplayMode() != format) {
            char* name;
            m->GetName((const char**)&name);
            CASPAR_LOG(info) << "discard displaymode " << name;
            m->Release();
        }
    }

    if (!m)
        CASPAR_THROW_EXCEPTION(user_error()
                               << msg_info("Device could not find requested video-format: " + std::to_string(format)));

    char* name;
    m->GetName((const char**)&name);
    CASPAR_LOG(info) << "using displaymode " << name;

    com_ptr<IDeckLinkDisplayMode> mode = wrap_raw<com_ptr>(m, true);

    BMDDisplayMode actualMode = bmdModeUnknown;
    BOOL           supported  = false;

    auto displayMode = mode->GetDisplayMode();
    if (FAILED(device->DoesSupportVideoMode(bmdVideoConnectionUnspecified,
                                            displayMode,
                                            pix_fmt,
                                            bmdNoVideoOutputConversion,
                                            flag,
                                            &actualMode,
                                            &supported)))
        CASPAR_THROW_EXCEPTION(caspar_exception()
                               << msg_info(L"Could not determine whether device supports requested video format: " +
                                           get_mode_name(mode)));
    else if (!supported)
        CASPAR_LOG(info) << L"Device may not support video-format: " << get_mode_name(mode);
    else if (actualMode != bmdModeUnknown && actualMode != displayMode)
        CASPAR_LOG(warning) << L"Device supports video-format with conversion: " << get_mode_name(mode) << "( "
                            << displayMode << ")"
                            << ", wanted format: " << actualMode;

    return mode;
}

void set_keyer(const com_iface_ptr<IDeckLinkProfileAttributes>& attributes,
               const com_iface_ptr<IDeckLinkKeyer>&             decklink_keyer,
               configuration::keyer_t                           keyer,
               const std::wstring&                              print)
{
    if (keyer == configuration::keyer_t::internal_keyer) {
        BOOL value = true;
        if (SUCCEEDED(attributes->GetFlag(BMDDeckLinkSupportsInternalKeying, &value)) && !value)
            CASPAR_LOG(error) << print << L" Failed to enable internal keyer.";
        else if (FAILED(decklink_keyer->Enable(FALSE)))
            CASPAR_LOG(error) << print << L" Failed to enable internal keyer.";
        else if (FAILED(decklink_keyer->SetLevel(255)))
            CASPAR_LOG(error) << print << L" Failed to set key-level to max.";
        else
            CASPAR_LOG(info) << print << L" Enabled internal keyer.";
    } else if (keyer == configuration::keyer_t::external_keyer) {
        BOOL value = true;
        if (SUCCEEDED(attributes->GetFlag(BMDDeckLinkSupportsExternalKeying, &value)) && !value)
            CASPAR_LOG(error) << print << L" Failed to enable external keyer.";
        else if (FAILED(decklink_keyer->Enable(TRUE)))
            CASPAR_LOG(error) << print << L" Failed to enable external keyer.";
        else if (FAILED(decklink_keyer->SetLevel(255)))
            CASPAR_LOG(error) << print << L" Failed to set key-level to max.";
        else
            CASPAR_LOG(info) << print << L" Enabled external keyer.";
    }
}

enum EOTF
{
    SDR = 0,
    HDR = 1,
    PQ  = 2,
    HLG = 3
};

struct ChromaticityCoordinates
{
    double RedX;
    double RedY;
    double GreenX;
    double GreenY;
    double BlueX;
    double BlueY;
    double WhiteX;
    double WhiteY;
};

struct HDRMetadata
{
    int64_t EOTF;
    double  maxDisplayMasteringLuminance;
    double  minDisplayMasteringLuminance;
    double  maxCLL;
    double  maxFALL;
};

const auto REC_709  = ChromaticityCoordinates{0.640, 0.330, 0.300, 0.600, 0.150, 0.060, 0.3127, 0.3290};
const auto REC_2020 = ChromaticityCoordinates{0.708, 0.292, 0.170, 0.797, 0.131, 0.046, 0.3127, 0.3290};

class decklink_frame
    : public IDeckLinkVideoFrame
    , public IDeckLinkVideoFrameMetadataExtensions
{
    core::video_format_desc format_desc_;
    std::shared_ptr<void>   data_;
    std::atomic<int>        ref_count_{0};
    int                     nb_samples_;
    const bool              hdr_;
    BMDFrameFlags           flags_;
    BMDPixelFormat          pix_fmt_;

  public:
    decklink_frame(std::shared_ptr<void> data, const core::video_format_desc& format_desc, int nb_samples, bool hdr)
        : format_desc_(format_desc)
        , data_(data)
        , nb_samples_(nb_samples)
        , hdr_(hdr)
        , flags_(hdr ? bmdFrameFlagDefault | bmdFrameContainsHDRMetadata : bmdFrameFlagDefault)
        , pix_fmt_(get_pixel_format(hdr))
    {
    }

    // IUnknown

    HRESULT STDMETHODCALLTYPE QueryInterface(const REFIID iid, LPVOID* ppv) override
    {
        /* Implementation from the SignalGenHDR example in the Decklink SDK */

        CFUUIDBytes iunknown;
        HRESULT     result = S_OK;

        if (ppv == nullptr)
            return E_INVALIDARG;

        // Initialise the return result
        *ppv = nullptr;

        iunknown = CFUUIDGetUUIDBytes(IUnknownUUID);
        if (std::memcmp(&iid, &iunknown, sizeof(REFIID)) == 0) {
            *ppv = this;
            AddRef();
        } else if (std::memcmp(&iid, &IID_IDeckLinkVideoFrame, sizeof(REFIID)) == 0) {
            *ppv = static_cast<IDeckLinkVideoFrame*>(this);
            AddRef();
        } else if (hdr_ && std::memcmp(&iid, &IID_IDeckLinkVideoFrameMetadataExtensions, sizeof(REFIID)) == 0) {
            *ppv = static_cast<IDeckLinkVideoFrameMetadataExtensions*>(this);
            AddRef();
        } else {
            result = E_NOINTERFACE;
        }

        return result;
    }

    ULONG STDMETHODCALLTYPE AddRef() override { return ++ref_count_; }

    ULONG STDMETHODCALLTYPE Release() override
    {
        if (--ref_count_ == 0) {
            delete this;

            return 0;
        }

        return ref_count_;
    }

    // IDecklinkVideoFrame

    long STDMETHODCALLTYPE GetWidth() override { return static_cast<long>(format_desc_.width); }
    long STDMETHODCALLTYPE GetHeight() override { return static_cast<long>(format_desc_.height); }
    long STDMETHODCALLTYPE GetRowBytes() override { return static_cast<long>(get_row_bytes(format_desc_, hdr_)); }
    BMDPixelFormat STDMETHODCALLTYPE GetPixelFormat() override { return pix_fmt_; }
    BMDFrameFlags STDMETHODCALLTYPE GetFlags() override { return flags_; }

    HRESULT STDMETHODCALLTYPE GetBytes(void** buffer) override
    {
        *buffer = data_.get();
        return S_OK;
    }

    HRESULT STDMETHODCALLTYPE GetTimecode(BMDTimecodeFormat format, IDeckLinkTimecode** timecode) override
    {
        return S_FALSE;
    }

    HRESULT STDMETHODCALLTYPE GetAncillaryData(IDeckLinkVideoFrameAncillary** ancillary) override { return S_FALSE; }

    int nb_samples() const { return nb_samples_; }

    // IDeckLinkVideoFrameMetadataExtensions
    HRESULT GetInt(BMDDeckLinkFrameMetadataID metadataID, int64_t* value)
    {
        HRESULT result = S_OK;

        switch (metadataID) {
            case bmdDeckLinkFrameMetadataHDRElectroOpticalTransferFunc:
                *value = EOTF::PQ;
                break;

            case bmdDeckLinkFrameMetadataColorspace:
                *value = bmdColorspaceRec709;
                break;

            default:
                value  = nullptr;
                result = E_INVALIDARG;
        }

        return result;
    }

    HRESULT GetFloat(BMDDeckLinkFrameMetadataID metadataID, double* value)
    {
        HRESULT result = S_OK;

        switch (metadataID) {
            case bmdDeckLinkFrameMetadataHDRDisplayPrimariesRedX:
                *value = REC_709.RedX;
                break;

            case bmdDeckLinkFrameMetadataHDRDisplayPrimariesRedY:
                *value = REC_709.RedY;
                break;

            case bmdDeckLinkFrameMetadataHDRDisplayPrimariesGreenX:
                *value = REC_709.GreenX;
                break;

            case bmdDeckLinkFrameMetadataHDRDisplayPrimariesGreenY:
                *value = REC_709.GreenY;
                break;

            case bmdDeckLinkFrameMetadataHDRDisplayPrimariesBlueX:
                *value = REC_709.BlueX;
                break;

            case bmdDeckLinkFrameMetadataHDRDisplayPrimariesBlueY:
                *value = REC_709.BlueY;
                break;

            case bmdDeckLinkFrameMetadataHDRWhitePointX:
                *value = REC_709.WhiteX;
                break;

            case bmdDeckLinkFrameMetadataHDRWhitePointY:
                *value = REC_709.WhiteY;
                break;

            case bmdDeckLinkFrameMetadataHDRMaxDisplayMasteringLuminance:
                *value = 1000.0;
                break;

            case bmdDeckLinkFrameMetadataHDRMinDisplayMasteringLuminance:
                *value = 0.005;
                break;

            case bmdDeckLinkFrameMetadataHDRMaximumContentLightLevel:
                *value = 1000.0;
                break;

            case bmdDeckLinkFrameMetadataHDRMaximumFrameAverageLightLevel:
                *value = 50.0;
                break;

            default:
                value  = nullptr;
                result = E_INVALIDARG;
        }

        return result;
    }

    HRESULT GetFlag(BMDDeckLinkFrameMetadataID, bool* value)
    {
        // Not expecting GetFlag
        *value = false;
        return E_INVALIDARG;
    }

    HRESULT GetString(BMDDeckLinkFrameMetadataID, const char** value)
    {
        // Not expecting GetString
        *value = nullptr;
        return E_INVALIDARG;
    }

    HRESULT GetBytes(BMDDeckLinkFrameMetadataID metadataID, void* buffer, uint32_t* bufferSize)
    {
        *bufferSize = 0;
        return E_INVALIDARG;
    }
};

struct decklink_consumer : public IDeckLinkVideoOutputCallback
{
    const int           channel_index_;
    const configuration config_;

    com_ptr<IDeckLink>                        decklink_      = get_device(config_.device_index);
    com_iface_ptr<IDeckLinkOutput>            output_        = iface_cast<IDeckLinkOutput>(decklink_);
    com_iface_ptr<IDeckLinkConfiguration>     configuration_ = iface_cast<IDeckLinkConfiguration>(decklink_);
    com_iface_ptr<IDeckLinkKeyer>             keyer_         = iface_cast<IDeckLinkKeyer>(decklink_, true);
    com_iface_ptr<IDeckLinkProfileAttributes> attributes_    = iface_cast<IDeckLinkProfileAttributes>(decklink_);

    std::mutex         exception_mutex_;
    std::exception_ptr exception_;

    const std::wstring            model_name_ = get_model_name(decklink_);
    const core::video_format_desc format_desc_;

    std::mutex                    buffer_mutex_;
    std::condition_variable       buffer_cond_;
    std::queue<core::const_frame> buffer_;
    int                           buffer_capacity_ = 1;

    const int buffer_size_ = config_.buffer_depth(); // Minimum buffer-size 3.

    long long video_scheduled_ = 0;
    long long audio_scheduled_ = 0;

    boost::circular_buffer<std::vector<int32_t>> audio_container_{static_cast<unsigned long>(buffer_size_ + 1)};

    spl::shared_ptr<diagnostics::graph> graph_;
    caspar::timer                       tick_timer_;
    reference_signal_detector           reference_signal_detector_{output_};
    std::atomic<int64_t>                scheduled_frames_completed_{0};

    com_ptr<IDeckLinkDisplayMode> mode_        = get_display_mode(output_,
                                                           format_desc_.format,
                                                           get_pixel_format(config_.hdr),
                                                           bmdVideoOutputFlagDefault,
                                                           config_.hdr);
    int                           field_count_ = mode_->GetFieldDominance() != bmdProgressiveFrame ? 2 : 1;

    std::atomic<bool> abort_request_{false};

    SwsContext* sws_;

    bool doFrame(std::shared_ptr<void>& image_data, std::vector<std::int32_t>& audio_data, bool topField)
    {
        core::const_frame frame(pop());

        if (abort_request_)
            return false;

        const auto is_16bit = frame.image_data(0).native_depth() == common::bit_depth::bit16;

        if (is_16bit) {
            if (!pack_video_data(image_data, frame))
                return false;
        } else {
            long long firstLine = topField ? 0 : 1;
            for (auto y = firstLine; y < format_desc_.height; y += field_count_) {
                std::memcpy(reinterpret_cast<char*>(image_data.get()) + y * format_desc_.width * 4,
                            frame.image_data(0).data() + y * format_desc_.width * 4,
                            (size_t)format_desc_.width * 4);
            }
        }

        audio_data.insert(audio_data.end(), frame.audio_data().begin(), frame.audio_data().end());

        return true;
    }

    bool pack_video_data(std::shared_ptr<void>& image_data, const core::const_frame& frame)
    {
        // swscale  AV_PIX_FMT_BGRA64LE -> AV_PIX_FMT_X2RGB10LE
        sws_ = sws_getCachedContext(sws_,
                                    format_desc_.width,
                                    format_desc_.height,
                                    AV_PIX_FMT_BGRA64LE,
                                    format_desc_.width,
                                    format_desc_.height,
                                    AV_PIX_FMT_X2BGR10LE, // AV_PIX_FMT_X2RGB10LE,
                                    SWS_POINT,
                                    nullptr,
                                    nullptr,
                                    nullptr);
        if (!sws_)
            return false;

        auto srcData       = frame.image_data(0).data();
        auto sourceRowSize = format_desc_.width * 8;
        auto destData      = reinterpret_cast<uint8_t*>(image_data.get());
        auto destStride    = get_linesize();
        sws_scale(sws_, &srcData, &sourceRowSize, 0, format_desc_.height, &destData, &destStride);

        return true;
    }

  public:
    decklink_consumer(const configuration& config, const core::video_format_desc& format_desc, int channel_index)
        : channel_index_(channel_index)
        , config_(config)
        , format_desc_(format_desc)
    {
        graph_->set_color("tick-time", diagnostics::color(0.0f, 0.6f, 0.9f));
        graph_->set_color("late-frame", diagnostics::color(0.6f, 0.3f, 0.3f));
        graph_->set_color("dropped-frame", diagnostics::color(0.3f, 0.6f, 0.3f));
        graph_->set_color("flushed-frame", diagnostics::color(0.4f, 0.3f, 0.8f));
        graph_->set_color("buffered-audio", diagnostics::color(0.9f, 0.9f, 0.5f));
        graph_->set_color("buffered-video", diagnostics::color(0.2f, 0.9f, 0.9f));

        if (mode_->GetFieldDominance() != bmdProgressiveFrame) {
            graph_->set_color("tick-time-f2", diagnostics::color(0.9f, 0.6f, 0.0f));
        }

        graph_->set_text(print());
        diagnostics::register_graph(graph_);

        enable_video(mode_->GetDisplayMode());

        if (config.embedded_audio) {
            enable_audio();
        }

        set_latency(configuration_, config.latency, print());
        set_keyer(attributes_, keyer_, config.keyer, print());

        if (config.hdr) {
            bool    flag  = false;
            int64_t value = 0;
            if (SUCCEEDED(attributes_->GetFlag(BMDDeckLinkSupportsHDRMetadata, &flag)) && !flag)
                CASPAR_LOG(error) << print() << L" Device does not support HDR metadata.";
            if (SUCCEEDED(attributes_->GetFlag(BMDDeckLinkSupportsColorspaceMetadata, &flag)) && !flag)
                CASPAR_LOG(warning) << print() << L" Device does not support colorspace metadata.";
            if (SUCCEEDED(attributes_->GetInt(BMDDeckLinkSupportedDynamicRange, &value)))
                CASPAR_LOG(info) << print() << L" Device supports following dymanic range standards: " << value;
        }

        if (config.embedded_audio) {
            output_->BeginAudioPreroll();
        }

        for (int n = 0; n < buffer_size_; ++n) {
            auto nb_samples = format_desc_.audio_cadence[n % format_desc_.audio_cadence.size()] * field_count_;
            if (config.embedded_audio) {
                schedule_next_audio(std::vector<int32_t>(nb_samples * format_desc_.audio_channels), nb_samples);
            }

            std::shared_ptr<void> image_data = allocate_frame_data();
            schedule_next_video(image_data, nb_samples);
        }

        if (config.embedded_audio) {
            output_->EndAudioPreroll();
        }

        start_playback();
    }

    ~decklink_consumer()
    {
        abort_request_ = true;
        buffer_cond_.notify_all();

        if (output_ != nullptr) {
            output_->StopScheduledPlayback(0, nullptr, 0);
            if (config_.embedded_audio) {
                output_->DisableAudioOutput();
            }
            output_->DisableVideoOutput();
        }

        if (sws_) {
            sws_freeContext(sws_);
            sws_ = nullptr;
        }
    }

    void enable_audio()
    {
        if (FAILED(output_->EnableAudioOutput(bmdAudioSampleRate48kHz,
                                              bmdAudioSampleType32bitInteger,
                                              format_desc_.audio_channels,
                                              bmdAudioOutputStreamTimestamped))) {
            CASPAR_THROW_EXCEPTION(caspar_exception() << msg_info(print() + L" Could not enable audio output."));
        }

        CASPAR_LOG(info) << print() << L" Enabled embedded-audio.";
    }

    void enable_video(BMDDisplayMode display_mode)
    {
        if (FAILED(output_->EnableVideoOutput(display_mode, bmdVideoOutputFlagDefault))) {
            CASPAR_THROW_EXCEPTION(caspar_exception() << msg_info(print() + L" Could not enable fill video output."));
        }

        if (FAILED(output_->SetScheduledFrameCompletionCallback(this))) {
            CASPAR_THROW_EXCEPTION(caspar_exception()
                                   << msg_info(print() + L" Failed to set fill playback completion callback.")
                                   << boost::errinfo_api_function("SetScheduledFrameCompletionCallback"));
        }
    }

    void start_playback()
    {
        if (FAILED(output_->StartScheduledPlayback(0, format_desc_.time_scale, 1.0))) {
            CASPAR_THROW_EXCEPTION(caspar_exception() << msg_info(print() + L" Failed to schedule fill playback."));
        }
    }

    HRESULT STDMETHODCALLTYPE QueryInterface(REFIID, LPVOID*) override { return E_NOINTERFACE; }
    ULONG STDMETHODCALLTYPE AddRef() override { return 1; }
    ULONG STDMETHODCALLTYPE Release() override { return 1; }

    HRESULT STDMETHODCALLTYPE ScheduledPlaybackHasStopped() override
    {
        CASPAR_LOG(info) << print() << L" Scheduled playback has stopped.";
        return S_OK;
    }

    HRESULT STDMETHODCALLTYPE ScheduledFrameCompleted(IDeckLinkVideoFrame*           completed_frame,
                                                      BMDOutputFrameCompletionResult result) override
    {
        try {
            auto elapsed     = tick_timer_.elapsed();
            int  fieldTimeMs = static_cast<int>(1000 / format_desc_.fps);
            // Calculate a time point for when a simulated second field action should occur for interlaced standards.
            // The tick_timer will run at frame (2x field) rate. If the tick_timer has been delayed because the machine
            // is busy this calculation reduces the delay before the second field so that it lands at the expected time,
            // giving the channel the full amount of time to process the following frame.
            std::chrono::high_resolution_clock::time_point f2TimePoint =
                std::chrono::high_resolution_clock::now() +
                std::chrono::milliseconds(std::max<int>(
                    0, std::min<int>(fieldTimeMs, fieldTimeMs + static_cast<int>(2.0 * fieldTimeMs - elapsed))));

            auto tick_time = elapsed * format_desc_.fps / field_count_ * 0.5;
            graph_->set_value("tick-time", tick_time);
            tick_timer_.restart();

            reference_signal_detector_.detect_change([this]() { return print(); });

            auto dframe = reinterpret_cast<decklink_frame*>(completed_frame);
            ++scheduled_frames_completed_;

            if (result == bmdOutputFrameDisplayedLate) {
                graph_->set_tag(diagnostics::tag_severity::WARNING, "late-frame");
                video_scheduled_ += format_desc_.duration * field_count_;
                audio_scheduled_ += dframe->nb_samples();
            } else if (result == bmdOutputFrameDropped) {
                graph_->set_tag(diagnostics::tag_severity::WARNING, "dropped-frame");
            } else if (result == bmdOutputFrameFlushed) {
                graph_->set_tag(diagnostics::tag_severity::WARNING, "flushed-frame");
            }

            {
                UINT32 buffered;
                output_->GetBufferedVideoFrameCount(&buffered);
                graph_->set_value("buffered-video", static_cast<double>(buffered) / config_.buffer_depth());

                if (config_.embedded_audio) {
                    output_->GetBufferedAudioSampleFrameCount(&buffered);
                    graph_->set_value("buffered-audio",
                                      static_cast<double>(buffered) /
                                          (format_desc_.audio_cadence[0] * field_count_ * config_.buffer_depth()));
                }
            }

            std::shared_ptr<void>     image_data = allocate_frame_data();
            std::vector<std::int32_t> audio_data;

            if (mode_->GetFieldDominance() != bmdProgressiveFrame) {
                if (!doFrame(image_data, audio_data, mode_->GetFieldDominance() == bmdUpperFieldFirst))
                    return E_FAIL;

                // Wait to pull frame for second field...
                std::this_thread::sleep_until(f2TimePoint);

                tick_time = tick_timer_.elapsed() * format_desc_.fps * 0.5;
                graph_->set_value("tick-time-f2", tick_time);

                if (!doFrame(image_data, audio_data, mode_->GetFieldDominance() != bmdUpperFieldFirst))
                    return E_FAIL;
            } else {
                if (!doFrame(image_data, audio_data, true))
                    return E_FAIL;
            }

            const auto nb_samples = static_cast<int>(audio_data.size()) / format_desc_.audio_channels;

            schedule_next_video(image_data, nb_samples);

            if (config_.embedded_audio) {
                schedule_next_audio(std::move(audio_data), nb_samples);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(exception_mutex_);
            exception_ = std::current_exception();
            return E_FAIL;
        }

        return S_OK;
    }

    int get_linesize() { return get_row_bytes(format_desc_, config_.hdr); }

    std::shared_ptr<void> allocate_frame_data()
    {
        auto alingment = config_.hdr ? 256 : 64;
        auto size      = config_.hdr ? get_linesize() * format_desc_.height : format_desc_.size;
        return std::shared_ptr<void>(aligned_alloc(alingment, size), free);
    }

    core::const_frame pop()
    {
        core::const_frame frame;
        {
            std::unique_lock<std::mutex> lock(buffer_mutex_);
            buffer_cond_.wait(lock, [&] { return !buffer_.empty() || abort_request_; });
            if (!abort_request_) {
                frame = std::move(buffer_.front());
                buffer_.pop();
            }
        }
        buffer_cond_.notify_all();
        return frame;
    }

    void schedule_next_audio(std::vector<std::int32_t> audio, int nb_samples)
    {
        // TODO (refactor) does ScheduleAudioSamples copy data?

        audio_container_.push_back(std::move(audio));

        if (FAILED(output_->ScheduleAudioSamples(audio_container_.back().data(),
                                                 nb_samples,
                                                 audio_scheduled_,
                                                 format_desc_.audio_sample_rate,
                                                 nullptr))) {
            CASPAR_LOG(error) << print() << L" Failed to schedule audio.";
        }

        audio_scheduled_ += nb_samples;
    }

    void schedule_next_video(std::shared_ptr<void> fill, int nb_samples)
    {
        auto frame =
            wrap_raw<com_ptr, IDeckLinkVideoFrame>(new decklink_frame(fill, format_desc_, nb_samples, config_.hdr));
        auto res = output_->ScheduleVideoFrame(
            get_raw(frame), video_scheduled_, format_desc_.duration * field_count_, format_desc_.time_scale);
        if (FAILED(res)) {
            CASPAR_LOG(error) << print() << L" Failed to schedule fill video. HRESULT = " << (unsigned int)res;
        }

        video_scheduled_ += format_desc_.duration * field_count_;
    }

    bool send(core::const_frame frame)
    {
        {
            std::lock_guard<std::mutex> lock(exception_mutex_);
            if (exception_ != nullptr) {
                std::rethrow_exception(exception_);
            }
        }

        if (frame) {
            std::unique_lock<std::mutex> lock(buffer_mutex_);
            buffer_cond_.wait(lock, [&] { return buffer_.size() < buffer_capacity_ || abort_request_; });
            buffer_.push(std::move(frame));
        }
        buffer_cond_.notify_all();

        return !abort_request_;
    }

    std::wstring print() const
    {
        return model_name_ + L" [" + std::to_wstring(channel_index_) + L"-" + std::to_wstring(config_.device_index) +
               L"|" + format_desc_.name + L"]";
    }
};

struct decklink_consumer_proxy : public core::frame_consumer
{
    const configuration                config_;
    std::unique_ptr<decklink_consumer> consumer_;
    core::video_format_desc            format_desc_;
    executor                           executor_;

  public:
    explicit decklink_consumer_proxy(const configuration& config)
        : config_(config)
        , executor_(L"decklink_consumer[" + std::to_wstring(config.device_index) + L"]")
    {
        executor_.begin_invoke([=] { com_initialize(); });
    }

    ~decklink_consumer_proxy()
    {
        executor_.invoke([=] {
            consumer_.reset();
            com_uninitialize();
        });
    }

    void initialize(const core::video_format_desc& format_desc, int channel_index) override
    {
        format_desc_ = format_desc;
        executor_.invoke([=] {
            consumer_.reset();
            consumer_.reset(new decklink_consumer(config_, format_desc, channel_index));
        });
    }

    std::future<bool> send(core::const_frame frame) override
    {
        return executor_.begin_invoke([=] { return consumer_->send(frame); });
    }

    std::wstring print() const override { return consumer_ ? consumer_->print() : L"[decklink_consumer]"; }

    std::wstring name() const override { return L"decklink"; }

    int index() const override { return 300 + config_.device_index; }

    bool has_synchronization_clock() const override { return true; }
};

spl::shared_ptr<core::frame_consumer> create_consumer(const std::vector<std::wstring>&                  params,
                                                      std::vector<spl::shared_ptr<core::video_channel>> channels,
                                                      common::bit_depth                                 depth)
{
    if (params.size() < 1 || !boost::iequals(params.at(0), L"DECKLINK")) {
        return core::frame_consumer::empty();
    }

    configuration config;

    if (params.size() > 1)
        config.device_index = std::stoi(params.at(1));

    if (contains_param(L"INTERNAL_KEY", params)) {
        config.keyer = configuration::keyer_t::internal_keyer;
    } else if (contains_param(L"EXTERNAL_KEY", params)) {
        config.keyer = configuration::keyer_t::external_keyer;
    } else {
        config.keyer = configuration::keyer_t::default_keyer;
    }

    if (contains_param(L"LOW_LATENCY", params)) {
        config.latency = configuration::latency_t::low_latency;
    }

    config.embedded_audio = contains_param(L"EMBEDDED_AUDIO", params);
    config.key_only       = contains_param(L"KEY_ONLY", params);

    config.hdr = (depth != common::bit_depth::bit8);

    return spl::make_shared<decklink_consumer_proxy>(config);
}

spl::shared_ptr<core::frame_consumer>
create_preconfigured_consumer(const boost::property_tree::wptree&               ptree,
                              std::vector<spl::shared_ptr<core::video_channel>> channels,
                              common::bit_depth                                 depth)
{
    configuration config;

    auto keyer = ptree.get(L"keyer", L"default");
    if (keyer == L"external") {
        config.keyer = configuration::keyer_t::external_keyer;
    } else if (keyer == L"internal") {
        config.keyer = configuration::keyer_t::internal_keyer;
    }

    auto latency = ptree.get(L"latency", L"default");
    if (latency == L"low") {
        config.latency = configuration::latency_t::low_latency;
    } else if (latency == L"normal") {
        config.latency = configuration::latency_t::normal_latency;
    }

    config.key_only          = ptree.get(L"key-only", config.key_only);
    config.device_index      = ptree.get(L"device", config.device_index);
    config.key_device_idx    = ptree.get(L"key-device", config.key_device_idx);
    config.embedded_audio    = ptree.get(L"embedded-audio", config.embedded_audio);
    config.base_buffer_depth = ptree.get(L"buffer-depth", config.base_buffer_depth);

    config.hdr = (depth != common::bit_depth::bit8);

    return spl::make_shared<decklink_consumer_proxy>(config);
}

}} // namespace caspar::decklink

/*
##############################################################################
Pre-rolling

Mail: 2011-05-09

Yoshan
BMD Developer Support
developer@blackmagic-design.com

-----------------------------------------------------------------------------

Thanks for your inquiry. The minimum number of frames that you can preroll
for scheduled playback is three frames for video and four frames for audio.
As you mentioned if you preroll less frames then playback will not start or
playback will be very sporadic. From our experience with Media Express, we
recommended that at least seven frames are prerolled for smooth playback.

Regarding the bmdDeckLinkConfigLowLatencyVideoOutput flag:
There can be around 3 frames worth of latency on scheduled output.
When the bmdDeckLinkConfigLowLatencyVideoOutput flag is used this latency is
reduced  or removed for scheduled playback. If the DisplayVideoFrameSync()
method is used, the bmdDeckLinkConfigLowLatencyVideoOutput setting will
guarantee that the provided frame will be output as soon the previous
frame output has been completed.
################################################################################
*/

/*
##############################################################################
Async DMA Transfer without redundant copying

Mail: 2011-05-10

Yoshan
BMD Developer Support
developer@blackmagic-design.com

-----------------------------------------------------------------------------

Thanks for your inquiry. You could try subclassing IDeckLinkMutableVideoFrame
and providing a pointer to your video buffer when GetBytes() is called.
This may help to keep copying to a minimum. Please ensure that the pixel
format is in bmdFormat10BitYUV, otherwise the DeckLink API / driver will
have to colourspace convert which may result in additional copying.
################################################################################
*/
