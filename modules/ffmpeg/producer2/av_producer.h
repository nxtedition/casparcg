#include <memory>

#include <core/frame/frame.h>
#include <core/frame/frame_factory.h>

#include <boost/optional.hpp>
#include <boost/rational.hpp>

namespace caspar {
namespace ffmpeg2 {

class AVProducer {
public:
    AVProducer(const std::shared_ptr<core::frame_factory>& frame_factory,
                  const core::video_format_desc& format_desc,
                  const std::string& filename,
                  const boost::optional<std::string> vfilter = boost::none,
                  const boost::optional<std::string> afilter = boost::none,
				  const boost::optional<int64_t> start = boost::none
		);

    boost::optional<core::draw_frame> next();

	boost::rational<int64_t> duration() const;

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

}  // namespace ffmpeg
}  // namespace caspar