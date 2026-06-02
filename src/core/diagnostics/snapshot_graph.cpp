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
 */

#include "../StdAfx.h"

#include "snapshot_graph.h"

#include "call_context.h"

#include <common/executor.h>
#include <common/log.h>
#include <common/utf.h>

#include <SFML/Graphics.hpp>

#include <boost/circular_buffer.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <tbb/concurrent_unordered_map.h>

#include <GL/glew.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace caspar { namespace core { namespace diagnostics { namespace snapshot {

#if SFML_VERSION_MAJOR >= 3

void register_sink(bool /*enabled*/, int /*retention_seconds*/) {}
bool take_snapshot(const std::wstring&) { return false; }
void shutdown() {}

#else

// The snapshot graph mirrors the on-screen-display graph (osd_graph.cpp) so that the rendered
// image conveys the same data with the same layout. The key differences are:
//   * data sampling is decoupled from drawing (a background tick samples; rendering only
//     happens on demand), and
//   * it renders to an offscreen sf::RenderTexture instead of a window.

static const int PREFERRED_VERTICAL_GRAPHS = 8;
static const int RENDERING_WIDTH           = 1024;
static const int RENDERING_HEIGHT          = RENDERING_WIDTH / PREFERRED_VERTICAL_GRAPHS;

// Height of the strip reserved above the stacked graphs for the time-axis labels, so they don't
// overlap the topmost plot.
static const int TIMESTAMP_BAND_HEIGHT = 20;

// The background sampling cadence (one recorded sample per line every TICK_INTERVAL_MS).
static const int TICK_INTERVAL_MS = 40;

// Default number of retained samples per line when no retention is configured
// (DEFAULT_BUFFER_SIZE * TICK_INTERVAL_MS ms of history ~= 41s).
static const size_t DEFAULT_BUFFER_SIZE = 1024;

// Runtime configuration, set once by register_sink() before any graph is created.
static std::atomic<bool>   g_snapshot_enabled{true};
static std::atomic<size_t> g_buffer_size{DEFAULT_BUFFER_SIZE};

sf::Color get_sfml_color(int color)
{
    return {static_cast<sf::Uint8>(color >> 24 & 255),
            static_cast<sf::Uint8>(color >> 16 & 255),
            static_cast<sf::Uint8>(color >> 8 & 255),
            static_cast<sf::Uint8>(color >> 0 & 255)};
}

auto& get_default_font()
{
    static sf::Font DEFAULT_FONT = []() {
        fs::path path{DIAG_FONT_PATH};
#ifdef __linux__
        if (!fs::exists(path)) {
            auto cmd = "fc-match --format=%{file} " + path.string();
            if (auto pipe = popen(cmd.data(), "r")) {
                char buf[128];
                path.clear();
                while (fgets(buf, sizeof(buf), pipe))
                    path += buf;
            }
        }
#endif
        sf::Font font;
        if (!font.loadFromFile(path.string()))
            CASPAR_THROW_EXCEPTION(file_not_found() << msg_info(DIAG_FONT_PATH " not found"));
        return font;
    }();

    return DEFAULT_FONT;
}

struct drawable
    : public sf::Drawable
    , public sf::Transformable
{
    virtual ~drawable() {}
    virtual void render(sf::RenderTarget& target, sf::RenderStates states) = 0;

    void draw(sf::RenderTarget& target, sf::RenderStates states) const override
    {
        states.transform *= getTransform();
        const_cast<drawable*>(this)->render(target, states);
    }
};

class line : public drawable
{
    size_t                                                 res_;
    boost::circular_buffer<sf::Vertex>                     line_data_;
    boost::circular_buffer<std::optional<sf::VertexArray>> line_tags_;

    std::atomic<float> tick_data_;
    std::atomic<bool>  tick_tag_;
    std::atomic<int>   color_;

    double x_delta_;

  public:
    line()
        : res_(std::max<size_t>(2, g_buffer_size.load()))
        , line_data_(res_)
        , line_tags_(res_)
        , tick_data_(-1.0f)
        , tick_tag_(false)
        , color_(0xFFFFFFFF)
        , x_delta_(1.0 / (static_cast<double>(res_) - 1.0))
    {
    }

    line(const line& other)
        : res_(other.res_)
        , line_data_(other.line_data_)
        , line_tags_(other.line_tags_)
        , tick_data_(other.tick_data_.load())
        , tick_tag_(other.tick_tag_.load())
        , color_(other.color_.load())
        , x_delta_(other.x_delta_)
    {
    }

    void set_value(float value) { tick_data_ = value; }

    void set_tag() { tick_tag_ = true; }

    void set_color(int color) { color_ = color; }

    int get_color() { return color_; }

    // Advance the circular buffer by one sample from the current value. Mutates; called only
    // by the recording tick (mirrors the mutating half of osd::line::render).
    void sample()
    {
        for (auto& vertex : line_data_)
            vertex.position.x -= x_delta_;

        for (auto& tag : line_tags_) {
            if (tag) {
                (*tag)[0].position.x -= x_delta_;
                (*tag)[1].position.x -= x_delta_;
            }
        }

        auto color = get_sfml_color(color_);
        color.a    = 255 * 0.8;
        line_data_.push_back(sf::Vertex(
            sf::Vector2f(get_insertion_xcoord(), std::max(0.1f, std::min(0.9f, (1.0f - tick_data_) * 0.8f + 0.1f))),
            color));

        if (tick_tag_) {
            sf::VertexArray vertical_dash(sf::LinesStrip);
            vertical_dash.append(sf::Vertex(sf::Vector2f(get_insertion_xcoord() - x_delta_, 0.f), color));
            vertical_dash.append(sf::Vertex(sf::Vector2f(get_insertion_xcoord() - x_delta_, 1.f), color));
            line_tags_.push_back(vertical_dash);
        } else
            line_tags_.push_back({});

        tick_tag_ = false;
    }

    // Read-only draw of the current buffer (mirrors the drawing half of osd::line::render).
    void render(sf::RenderTarget& target, sf::RenderStates states) override
    {
        if (tick_data_ > -0.5) {
            auto array_one = line_data_.array_one();
            auto array_two = line_data_.array_two();
            // since boost::circular_buffer guarantees two contiguous views of the buffer we can provide raw access to
            // SFML, which can use glDrawArrays.
            target.draw(array_one.first, static_cast<unsigned int>(array_one.second), sf::LinesStrip, states);
            target.draw(array_two.first, static_cast<unsigned int>(array_two.second), sf::LinesStrip, states);

            if (array_one.second > 0 && array_two.second > 0) {
                // Connect the gap between the arrays
                sf::VertexArray connecting_line(sf::LinesStrip);
                connecting_line.append(*(array_one.first + array_one.second - 1));
                connecting_line.append(*array_two.first);
                target.draw(connecting_line, states);
            }
        } else {
            glEnable(GL_LINE_STIPPLE);
            glLineStipple(3, 0xAAAA);

            for (size_t n = 0; n < line_tags_.size(); ++n) {
                if (line_tags_[n]) {
                    target.draw(*line_tags_[n], states);
                }
            }

            glDisable(GL_LINE_STIPPLE);
        }
    }

  private:
    double get_insertion_xcoord() const { return line_data_.empty() ? 1.0 : line_data_.back().position.x + x_delta_; }
};

struct graph
    : public drawable
    , public caspar::diagnostics::spi::graph_sink
    , public std::enable_shared_from_this<graph>
{
    call_context                                     context_ = call_context::for_thread();
    tbb::concurrent_unordered_map<std::string, line> lines_;

    std::mutex   mutex_;
    std::wstring text_;
    bool         auto_reset_ = false;

    graph() {}

    void activate() override;

    void set_text(const std::wstring& value) override
    {
        auto                        temp = value;
        std::lock_guard<std::mutex> lock(mutex_);
        text_ = std::move(temp);
    }

    void set_value(const std::string& name, double value) override
    {
        lines_[name].set_value(static_cast<float>(value));
    }

    void set_tag(caspar::diagnostics::tag_severity /*severity*/, const std::string& name) override
    {
        lines_[name].set_tag();
    }

    void set_color(const std::string& name, int color) override { lines_[name].set_color(color); }

    void auto_reset() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto_reset_ = true;
    }

    // Sample all lines for one tick. The OSD resets auto-reset graphs after drawing; since the
    // snapshot draws rarely, the reset is done here (after sampling) to keep per-tick semantics
    // for auto-reset graphs (e.g. drop counters).
    void sample_tick()
    {
        bool auto_reset;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto_reset = auto_reset_;
        }

        for (auto it = lines_.begin(); it != lines_.end(); ++it) {
            it->second.sample();
            if (auto_reset)
                it->second.set_value(0.0f);
        }
    }

  private:
    void render(sf::RenderTarget& target, sf::RenderStates states) override
    {
        const size_t text_size   = 15;
        const size_t text_margin = 2;
        const size_t text_offset = (text_size + text_margin * 2) * 2;

        std::wstring text_str;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            text_str = text_;
        }

        sf::Text text(text_str.c_str(), get_default_font(), text_size);
        text.setStyle(sf::Text::Italic);
        text.move(text_margin, text_margin);

        target.draw(text, states);

        if (context_.video_channel != -1) {
            auto ctx_str = std::to_string(context_.video_channel);

            if (context_.layer != -1)
                ctx_str += "-" + std::to_string(context_.layer);

            sf::Text context_text(ctx_str, get_default_font(), text_size);
            context_text.setStyle(sf::Text::Italic);
            context_text.move(RENDERING_WIDTH - text_margin - 5 - context_text.getLocalBounds().width, text_margin);

            target.draw(context_text, states);
        }

        float x_offset = text_margin;

        for (auto it = lines_.begin(); it != lines_.end(); ++it) {
            sf::Text line_text(it->first, get_default_font(), text_size);
            line_text.setPosition(x_offset, text_margin + text_offset / 2);
            line_text.setColor(get_sfml_color(it->second.get_color()));
            target.draw(line_text, states);
            x_offset += line_text.getLocalBounds().width + text_margin * 2;
        }

        static const auto rect = []() {
            sf::RectangleShape r(sf::Vector2f(RENDERING_WIDTH, RENDERING_HEIGHT - 2));
            r.setFillColor(sf::Color(255, 255, 255, 51));
            r.setOutlineThickness(0.00f);
            r.move(0, 1);
            return r;
        }();
        target.draw(rect, states);

        states.transform.translate(0, text_offset)
            .scale(RENDERING_WIDTH,
                   RENDERING_HEIGHT *
                       (static_cast<float>(RENDERING_HEIGHT - text_offset) / static_cast<float>(RENDERING_HEIGHT)));

        static const sf::Color       guide_color(255, 255, 255, 127);
        static const sf::VertexArray middle_guide = []() {
            sf::VertexArray result(sf::LinesStrip);
            result.append(sf::Vertex(sf::Vector2f(0.0f, 0.5f), guide_color));
            result.append(sf::Vertex(sf::Vector2f(1.0f, 0.5f), guide_color));
            return result;
        }();
        static const sf::VertexArray bottom_guide = []() {
            sf::VertexArray result(sf::LinesStrip);
            result.append(sf::Vertex(sf::Vector2f(0.0f, 0.9f), guide_color));
            result.append(sf::Vertex(sf::Vector2f(1.0f, 0.9f), guide_color));
            return result;
        }();
        static const sf::VertexArray top_guide = []() {
            sf::VertexArray result(sf::LinesStrip);
            result.append(sf::Vertex(sf::Vector2f(0.0f, 0.1f), guide_color));
            result.append(sf::Vertex(sf::Vector2f(1.0f, 0.1f), guide_color));
            return result;
        }();

        glEnable(GL_LINE_STIPPLE);
        glLineStipple(3, 0xAAAA);

        target.draw(middle_guide, states);
        target.draw(bottom_guide, states);
        target.draw(top_guide, states);

        glDisable(GL_LINE_STIPPLE);

        for (auto it = lines_.begin(); it != lines_.end(); ++it) {
            target.draw(it->second, states);
        }
    }
};

// Format a wall-clock instant as a human-readable string with millisecond accuracy,
// e.g. "2026-06-02 14:30:00.123".
static std::string format_timestamp(const boost::posix_time::ptime& t)
{
    if (t.is_not_a_date_time())
        return "";

    // to_iso_extended_string yields "YYYY-MM-DDTHH:MM:SS[.ffffff]".
    std::string s = boost::posix_time::to_iso_extended_string(t);

    if (auto t_pos = s.find('T'); t_pos != std::string::npos)
        s[t_pos] = ' ';

    if (auto dot = s.find('.'); dot != std::string::npos) {
        // Truncate the fractional seconds to milliseconds (3 digits).
        s.resize(std::min(s.size(), dot + 4));
    } else {
        s += ".000";
    }

    return s;
}

class context
{
    executor                          executor_{L"diagnostics-snapshot"};
    std::list<std::weak_ptr<graph>>   graphs_;
    bool                              ticking_ = false;

    // Wall-clock time of the most recent sample and the total number of samples taken. Both are
    // only touched on the executor thread (sampling and rendering are serialised there), so no
    // extra synchronisation is required.
    boost::posix_time::ptime last_tick_time_;
    std::uint64_t            tick_count_ = 0;

  public:
    static void register_graph(const std::shared_ptr<graph>& g)
    {
        if (!g)
            return;

        get_instance()->executor_.begin_invoke([=] { get_instance()->do_register_graph(g); });
    }

    static bool take_snapshot(const std::wstring& file_path)
    {
        return get_instance()->executor_.invoke([&] { return get_instance()->render_to_file(file_path); });
    }

    static void shutdown() { get_instance().reset(); }

  private:
    context() {}

    void do_register_graph(const std::shared_ptr<graph>& g)
    {
        graphs_.push_back(g);
        prune();

        if (!ticking_) {
            ticking_ = true;
            tick();
        }
    }

    void prune()
    {
        auto it = graphs_.begin();
        while (it != graphs_.end()) {
            if (it->lock())
                ++it;
            else
                it = graphs_.erase(it);
        }
    }

    void tick()
    {
        prune();

        last_tick_time_ = boost::posix_time::microsec_clock::local_time();
        ++tick_count_;

        for (auto& weak : graphs_) {
            if (auto g = weak.lock())
                g->sample_tick();
        }

        if (executor_.is_running())
            executor_.begin_invoke([this] { tick(); });

        std::this_thread::sleep_for(std::chrono::milliseconds(TICK_INTERVAL_MS));
    }

    // Annotate the time axis in the reserved band above the graphs: the right label carries the
    // timestamp of the most recent sample, the left label that of the oldest retained sample. Both
    // carry millisecond accuracy and reflect the actually collected data.
    void draw_time_axis_labels(sf::RenderTarget& target)
    {
        if (tick_count_ == 0 || last_tick_time_.is_not_a_date_time())
            return;

        const size_t res    = std::max<size_t>(2, g_buffer_size.load());
        const auto   filled = std::min<std::uint64_t>(tick_count_, res);

        const auto end_time   = last_tick_time_;
        const auto begin_time =
            end_time - boost::posix_time::milliseconds(static_cast<long>((filled - 1) * TICK_INTERVAL_MS));

        const unsigned int label_size = 13;
        const float        margin     = 3.f;
        // Vertically centre the labels within the reserved band.
        const float label_y = (TIMESTAMP_BAND_HEIGHT - static_cast<float>(label_size)) / 2.f;

        const sf::Color label_color(255, 255, 0, 220);

        sf::Text end_label(format_timestamp(end_time), get_default_font(), label_size);
        end_label.setColor(label_color);
        const float end_x = RENDERING_WIDTH - margin - end_label.getLocalBounds().width;
        end_label.setPosition(end_x, label_y);

        // The graph maps the whole buffer (res samples) across the full width with the newest
        // sample at the right edge. Until the buffer fills, the oldest sample sits inset from the
        // left, so align the begin label with that sample's x-position (== left edge once full).
        if (filled > 1) {
            const float oldest_norm_x = 1.0f - static_cast<float>(filled - 1) / static_cast<float>(res - 1);

            sf::Text begin_label(format_timestamp(begin_time), get_default_font(), label_size);
            begin_label.setColor(label_color);

            float begin_x = oldest_norm_x * RENDERING_WIDTH;
            // Keep the label on-screen and clear of the right-aligned end label.
            begin_x = std::max(margin, std::min(begin_x, end_x - begin_label.getLocalBounds().width - margin * 2));
            begin_label.setPosition(begin_x, label_y);

            target.draw(begin_label);
        }

        target.draw(end_label);
    }

    bool render_to_file(const std::wstring& file_path)
    {
        std::vector<std::shared_ptr<graph>> graphs;
        for (auto& weak : graphs_) {
            if (auto g = weak.lock())
                graphs.push_back(g);
        }

        const unsigned int count  = std::max<unsigned int>(1, static_cast<unsigned int>(graphs.size()));
        const unsigned int width  = RENDERING_WIDTH;
        const unsigned int height = TIMESTAMP_BAND_HEIGHT + RENDERING_HEIGHT * count;

        try {
            sf::RenderTexture render_texture;
            if (!render_texture.create(width, height)) {
                CASPAR_LOG(error) << L"diagnostics snapshot: failed to create render texture (no GL context?)";
                return false;
            }

            render_texture.setActive(true);
            glEnable(GL_BLEND);
            glEnable(GL_LINE_SMOOTH);
            glHint(GL_LINE_SMOOTH_HINT, GL_NICEST);
            glBlendFunc(GL_SRC_ALPHA, GL_ONE_MINUS_SRC_ALPHA);

            render_texture.clear(sf::Color(0, 0, 0, 255));

            int n = 0;
            for (auto& g : graphs) {
                g->setPosition(0.0f, static_cast<float>(TIMESTAMP_BAND_HEIGHT + n * RENDERING_HEIGHT));
                render_texture.draw(*g);
                ++n;
            }

            draw_time_axis_labels(render_texture);

            render_texture.display();

            return render_texture.getTexture().copyToImage().saveToFile(u8(file_path));
        } catch (...) {
            CASPAR_LOG_CURRENT_EXCEPTION();
            return false;
        }
    }

    static std::unique_ptr<context>& get_instance()
    {
        static auto impl = std::unique_ptr<context>(new context);
        return impl;
    }
};

void graph::activate() { context::register_graph(shared_from_this()); }

void register_sink(bool enabled, int retention_seconds)
{
    g_snapshot_enabled = enabled;

    if (retention_seconds > 0)
        g_buffer_size = std::max<size_t>(2, static_cast<size_t>(retention_seconds) * 1000 / TICK_INTERVAL_MS);

    // When disabled, skip registering the sink entirely: no graphs are recorded and the background
    // sampling tick never starts, so the feature incurs no overhead.
    if (!enabled)
        return;

    caspar::diagnostics::spi::register_sink_factory([] { return spl::make_shared<graph>(); });
}

bool take_snapshot(const std::wstring& file_path)
{
    if (!g_snapshot_enabled)
        return false;

    return context::take_snapshot(file_path);
}

void shutdown() { context::shutdown(); }

#endif

}}}} // namespace caspar::core::diagnostics::snapshot
