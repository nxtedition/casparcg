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

#include "../StdAfx.h"

#include "ffmpeg_producer2.h"

#include "../producer/util/util.h"
#include "av_producer.h"

#include "../ffmpeg.h"
#include "../ffmpeg_error.h"

#include <common/param.h>
#include <common/diagnostics/graph.h>
#include <common/future.h>

#include <core/frame/draw_frame.h>
#include <core/help/help_repository.h>
#include <core/help/help_sink.h>
#include <core/producer/media_info/media_info.h>
#include <core/producer/framerate/framerate_producer.h>
#include <core/frame/frame_factory.h>

#include <tbb/concurrent_queue.h>
#include <tbb/atomic.h>

#include <boost/thread.hpp>
#include <boost/timer.hpp>

#include <atomic>
#include <future>
#include <queue>

namespace caspar { namespace ffmpeg2 {

typedef std::vector<std::pair<std::string, std::string>> ffmpeg_options;

// HACK
std::wstring get_relative_or_original(
	const std::wstring& filename,
	const boost::filesystem::path& relative_to)
{
	boost::filesystem::path file(filename);
	auto result = file.filename().wstring();

	boost::filesystem::path current_path = file;

	while (true)
	{
		current_path = current_path.parent_path();

		if (boost::filesystem::equivalent(current_path, relative_to))
			break;

		if (current_path.empty())
			return filename;

		result = current_path.filename().wstring() + L"/" + result;
	}

	return result;
}

// TODO duration

struct ffmpeg_producer : public core::frame_producer_base
{
	const std::wstring 									filename_;
	// TODO
	const std::wstring 									path_relative_to_media_ = get_relative_or_original(filename_, env::media_folder());
	spl::shared_ptr<core::frame_factory> 				frame_factory_;
	core::video_format_desc								format_desc_;

	std::mutex											param_mutex;
	std::wstring										vfilter_;
	std::wstring										afilter_;
	boost::optional<std::int64_t>						start_;
	boost::optional<std::int64_t>						duration_;
	bool												loop_;

	std::atomic<double>									frame_time_;
	std::atomic<std::int64_t>							frame_count_ = 0;
	std::int64_t										frame_number_ = 0;

	std::mutex											exception_mutex_;
	std::exception_ptr									exception_ptr;

	core::monitor::subject  							monitor_subject_;
	core::draw_frame									curr_frame_ = core::draw_frame::empty();
	const spl::shared_ptr<diagnostics::graph>			graph_;
	timer												frame_timer_;
	core::constraints									constraints_;

	std::atomic<bool>									abort_request_;
	tbb::concurrent_bounded_queue<core::draw_frame>		buffer_;
	boost::thread										thread_;

	core::draw_frame									eof_ = core::draw_frame();
public:
	explicit ffmpeg_producer(
			const spl::shared_ptr<core::frame_factory>& frame_factory,
			const core::video_format_desc& format_desc,
			const std::wstring& filename,
			const std::wstring& vfilter,
			const std::wstring& afilter,
			const boost::optional<std::int64_t> start,
			const boost::optional<std::int64_t> duration,
			bool loop)
		: format_desc_(format_desc)
		, filename_(filename)
		, frame_factory_(frame_factory)
		, vfilter_(vfilter)
		, afilter_(afilter)
		, start_(start)
		, duration_(duration)
		, loop_(loop)
		, thread_([this] { run(); })
	{
		abort_request_ = false;
		diagnostics::register_graph(graph_);
		graph_->set_color("frame-time", diagnostics::color(0.1f, 1.0f, 0.1f));
		graph_->set_color("underflow", diagnostics::color(0.6f, 0.3f, 0.9f));
		graph_->set_color("buffer-count", diagnostics::color(0.7f, 0.4f, 0.4f));
		graph_->set_text(print());

		frame_count_ = std::numeric_limits<std::int64_t>::max();

		// TODO this is not entirely correct...
		frame_number_ = 1;

		// HACK
		{
			for (int n = 0; n < 50 && !buffer_.try_pop(curr_frame_); ++n)
				boost::this_thread::sleep(boost::posix_time::milliseconds(100));

			{
				std::lock_guard<std::mutex> lock(exception_mutex_);
				if (exception_ptr)
					std::rethrow_exception(exception_ptr);
			}
		}

		// TODO
		// constraints_.width.set(video_decoder_->width());
		// constraints_.height.set(video_decoder_->height());
	}

	~ffmpeg_producer()
	{
		abort_request_ = true;
		buffer_.abort();
		thread_.join();
	}

	void run()
	{
		buffer_.set_capacity(32);

		std::unique_ptr<av_producer_t> producer;

		try
		{
			while (!abort_request_)
			{
				if (!producer)
				{
					std::lock_guard<std::mutex> lock(param_mutex);
					producer.reset(new av_producer_t(frame_factory_, format_desc_, u8(filename_), u8(vfilter_), u8(afilter_), start_));
				}

				const boost::timer frame_timer;				
				const auto frame = producer->next();
				frame_time_ = frame_timer.elapsed();
				graph_->set_value("frame-time", frame_timer.elapsed() * boost::rational_cast<double>(format_desc_.framerate) * 0.5);

				if (frame)
				{
					frame_count_ = boost::rational_cast<int64_t>(producer->duration() * format_desc_.time_scale / format_desc_.duration);
					buffer_.push(std::move(*frame));
					graph_->set_value("buffer-count", static_cast<double>(buffer_.size()) / static_cast<double>(buffer_.capacity()));
				}
				else if (loop_)
				{
					// TODO reset frame_number
					producer.reset();
				}
			}
		}
		catch (tbb::user_abort&)
		{
		}
		catch (...)
		{
			{
				std::lock_guard<std::mutex> lock(exception_mutex_);
				exception_ptr = std::current_exception();
			}
			CASPAR_LOG_CURRENT_EXCEPTION();
		}
	}

	// frame_producer

	core::draw_frame receive_impl() override
	{
		core::draw_frame frame = curr_frame_;
		curr_frame_ = core::draw_frame::empty();

		if (buffer_.try_pop(curr_frame_))
		{			
			graph_->set_value("buffer-count", static_cast<double>(buffer_.size()) / static_cast<double>(buffer_.capacity()));
			frame_number_ += 1;
		}
		else
			graph_->set_tag(diagnostics::tag_severity::WARNING, "underflow");

		send_osc();
		graph_->set_text(print());
		
		return frame;
	}
	
	core::constraints& pixel_constraints() override
	{
		return constraints_;
	}

	uint32_t nb_frames() const override
	{
		return loop_ ? std::numeric_limits<std::uint32_t>::max() : static_cast<std::uint32_t>(frame_count_);
	}

	std::future<std::wstring> call(const std::vector<std::wstring>& params) override
	{
		std::wstring result;

		std::wstring cmd = params.at(0);
		std::wstring value;
		if (params.size() > 1)
			value = params.at(1);

		const auto in_tb = AVRational{ format_desc_.duration, format_desc_.time_scale };
		const auto out_tb = AVRational{ 1, AV_TIME_BASE };

		const auto to_pts = [&](std::wstring value) 
		{
			return av_rescale_q(boost::lexical_cast<std::int64_t>(value), in_tb, out_tb);
		};

		const auto from_pts = [&](int64_t value) 
		{
			return boost::lexical_cast<std::wstring>(av_rescale_q(static_cast<int64_t>(value), out_tb, in_tb));
		};

		if (boost::iequals(cmd, L"loop"))
		{
			std::lock_guard<std::mutex> lock(param_mutex);

			if (!value.empty())
				loop_ = boost::lexical_cast<bool>(value);

			result = boost::lexical_cast<std::wstring>(loop_);
		}
		else if (boost::iequals(cmd, L"in") || boost::iequals(cmd, L"start"))
		{
			std::lock_guard<std::mutex> lock(param_mutex);

			if (!value.empty())
				start_ = to_pts(value);

			result = start_ ? from_pts(*start_) : L"0";
		}
		else if (boost::iequals(cmd, L"out"))
		{
			std::lock_guard<std::mutex> lock(param_mutex);

			if (!value.empty())
				duration_ = to_pts(value) - start_.get_value_or(0);

			const auto duration = duration_.get_value_or(frame_count_);
			result = duration != std::numeric_limits<std::int64_t>::max()
				? from_pts(start_.get_value_or(0) + duration)
				: L"-1";
		}
		else if (boost::iequals(cmd, L"length"))
		{
			std::lock_guard<std::mutex> lock(param_mutex);

			if (!value.empty())
				duration_ = to_pts(value);

			const auto duration = duration_.get_value_or(frame_count_);
			result = duration != std::numeric_limits<std::int64_t>::max() 
				? from_pts(duration)
				: L"-1";
		}
		// TODO
		// else if (boost::iequals(cmd, L"seek") && !value.empty())
		// {
		// 	auto nb_frames = file_nb_frames();

		// 	int64_t seek;
		// 	if (boost::iequals(value, L"rel"))
		// 		seek = file_frame_number();
		// 	else if (boost::iequals(value, L"in"))
		// 		seek = input_.in();
		// 	else if (boost::iequals(value, L"out"))
		// 		seek = input_.out();
		// 	else if (boost::iequals(value, L"end"))
		// 		seek = nb_frames;
		// 	else
		// 		seek = boost::lexical_cast<int64_t>(value);

		// 	if (params.size() > 2)
		// 		seek += boost::lexical_cast<int64_t>(params.at(2));

		// 	if (seek < 0)
		// 		seek = 0;
		// 	else if (seek >= nb_frames)
		// 		seek = nb_frames - 1;

		// 	input_.seek(static_cast<uint32_t>(seek));
		// }
		else
			CASPAR_THROW_EXCEPTION(invalid_argument());

		return make_ready_future(std::move(result));
	}

	boost::property_tree::wptree info() const override
	{
		boost::property_tree::wptree info;
		info.add(L"type", L"ffmpeg-producer");
		info.add(L"filename", filename_);
		// TODO
		// info.add(L"width", video_decoder_ ? video_decoder_->width() : 0);
		// info.add(L"height", video_decoder_ ? video_decoder_->height() : 0);
		// info.add(L"progressive", video_decoder_ ? video_decoder_->is_progressive() : false);
		info.add(L"fps", format_desc_.fps);
		info.add(L"loop", loop_);
		info.add(L"file-frame-number", frame_number_);
		info.add(L"file-nb-frames", duration_.get_value_or(frame_count_ == std::numeric_limits<int64_t>::max() ? -1 : frame_count_));
		// TODO
		// info.add(L"frame-number", frame_number_);
		// info.add(L"nb-frames", frame_count_ == std::numeric_limits<int64_t>::max() ? -1 : frame_count_.load());
		return info;
	}

	std::wstring print() const override
	{
		return L"ffmpeg[" + filename_ + L"|" + boost::lexical_cast<std::wstring>(frame_number_) + L"/" + boost::lexical_cast<std::wstring>(duration_.get_value_or(frame_count_)) + L"]";
	}

	std::wstring name() const override
	{
		return L"ffmpeg";
	}

	core::monitor::subject& monitor_output()
	{
		return monitor_subject_;
	}

	void send_osc()
	{
		const auto fps = format_desc_.fps;

		monitor_subject_
			<< core::monitor::message("/profiler/time") % frame_time_ % (1.0 / fps)
			<< core::monitor::message("/file/time") % (frame_number_ / fps) % (frame_count_ / fps)
			<< core::monitor::message("/file/frame") % static_cast<int32_t>(frame_number_) % static_cast<int32_t>(frame_count_)
			<< core::monitor::message("/file/fps") % fps
			<< core::monitor::message("/file/path") % path_relative_to_media_
			<< core::monitor::message("/loop") % loop_;
	}
};

spl::shared_ptr<core::frame_producer> create_producer(
		const core::frame_producer_dependencies& dependencies,
		const std::vector<std::wstring>& params,
		const spl::shared_ptr<core::media_info_repository>& info_repo)
{
	auto file_or_url = params.at(0);

	if (!boost::contains(file_or_url, L"://"))
	{
		file_or_url = ffmpeg::probe_stem(env::media_folder() + L"/" + file_or_url, false);
	}

	if (file_or_url.empty())
		return core::frame_producer::empty();

	constexpr auto uint32_max = std::numeric_limits<uint32_t>::max();

	auto loop = contains_param(L"LOOP", params);

	auto in = get_param(L"SEEK", params, static_cast<uint32_t>(0)); // compatibility
	in = get_param(L"IN", params, in);

	auto out = get_param(L"LENGTH", params, uint32_max);
	if (out < uint32_max - in)
		out += in;
	else
		out = uint32_max;
	out = get_param(L"OUT", params, out);

	auto filter_str = get_param(L"FILTER", params, L"");
	auto custom_channel_order = get_param(L"CHANNEL_LAYOUT", params, L"");

	boost::ireplace_all(filter_str, L"DEINTERLACE_BOB", L"YADIF=1:-1");
	boost::ireplace_all(filter_str, L"DEINTERLACE_LQ", L"SEPARATEFIELDS");
	boost::ireplace_all(filter_str, L"DEINTERLACE", L"YADIF=0:-1");

	ffmpeg_options vid_params;
	bool haveFFMPEGStartIndicator = false;
	for (size_t i = 0; i < params.size() - 1; ++i)
	{
		if (!haveFFMPEGStartIndicator && params[i] == L"--")
		{
			haveFFMPEGStartIndicator = true;
			continue;
		}
		if (haveFFMPEGStartIndicator)
		{
			auto name = u8(params.at(i++)).substr(1);
			auto value = u8(params.at(i));
			vid_params.push_back(std::make_pair(name, value));
		}
	}

	const auto in_tb = AVRational{ dependencies.format_desc.duration,  dependencies.format_desc.time_scale };
	const auto out_tb = AVRational{ 1, AV_TIME_BASE };

	boost::optional<std::int64_t> start;
	boost::optional<std::int64_t> duration;

	if (in != 0)
		start = av_rescale_q(static_cast<int64_t>(in), in_tb, out_tb);

	if (out != uint32_max)
		duration = av_rescale_q(static_cast<int64_t>(out - in), in_tb, out_tb);

	// TODO
	custom_channel_order;
	vid_params;

	auto vfilter = get_param(L"VF", params, filter_str);
	auto afilter = get_param(L"AF", params, get_param(L"FILTER", params, L""));

	auto producer = spl::make_shared<ffmpeg_producer>(
		dependencies.frame_factory,
		dependencies.format_desc,
		file_or_url,
		vfilter,
		afilter,
		start,
		duration,
		loop);

	return core::create_destroy_proxy(std::move(producer));
}

}}
