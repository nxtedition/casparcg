#include "../../StdAfx.h"

#if defined(_MSC_VER)
#pragma warning (disable : 4714) // marked as __forceinline not inlined
#endif

#include "cg_producer.h"
#include "flash_producer.h"

#include "../../producer/frame_producer_device.h"
#include "../../video/video_format.h"
#include "../../processor/frame.h"
#include "../../Server.h"

#include <boost/filesystem.hpp>
#include <boost/assign.hpp>
#include <tbb/concurrent_unordered_map.h>
		
namespace caspar { namespace core { namespace flash{

struct flash_cg_proxy
{
	virtual std::wstring add(int layer, const std::wstring& templateName,  bool playOnLoad, const std::wstring& startFromLabel = TEXT(""), const std::wstring& data = TEXT("")) = 0;
	virtual std::wstring remove(int layer) = 0;
	virtual std::wstring play(int layer)  = 0;
	virtual std::wstring stop(int layer, unsigned int mixOutDuration) = 0;
	virtual std::wstring next(int layer) = 0;
	virtual std::wstring update(int layer, const std::wstring& data) = 0;
	virtual std::wstring invoke(int layer, const std::wstring& label) = 0;
};

struct flash_cg_proxy16 : public flash_cg_proxy
{		
	virtual std::wstring add(int layer, const std::wstring& templateName, bool playOnLoad, const std::wstring& label, const std::wstring& data) 
	{
		std::wstringstream flashParam;
		std::wstring::size_type pos = templateName.find('.');
		std::wstring filename = (pos != std::wstring::npos) ? templateName.substr(0, pos) : templateName;		
		flashParam << TEXT("<invoke name=\"Add\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number><string>") << filename << TEXT("</string><number>0</number>") << (playOnLoad?TEXT("<true/>"):TEXT("<false/>")) << TEXT("<string>") << label << TEXT("</string><string><![CDATA[ ") << data << TEXT(" ]]></string></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring remove(int layer) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Delete\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring play(int layer) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Play\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring stop(int layer, unsigned int mixOutDuration) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Stop\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number><number>") << mixOutDuration << TEXT("</number></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring next(int layer) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Next\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring update(int layer, const std::wstring& data) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"SetData\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number><string><![CDATA[ ") << data << TEXT(" ]]></string></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring invoke(int layer, const std::wstring& label) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"ExecuteMethod\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number><string>") << label << TEXT("</string></arguments></invoke>");
		return flashParam.str();
	}
};

struct flash_cg_proxy17 : public flash_cg_proxy
{
	virtual std::wstring add(int layer, const std::wstring& templateName, bool playOnLoad, const std::wstring& label, const std::wstring& data) 
	{
		std::wstringstream flashParam;

		std::wstring::size_type pos = templateName.find('.');
		std::wstring filename = (pos != std::wstring::npos) ? templateName.substr(0, pos) : templateName;
		
		flashParam << TEXT("<invoke name=\"Add\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number><string>") << filename << TEXT("</string>") << (playOnLoad?TEXT("<true/>"):TEXT("<false/>")) << TEXT("<string>") << label << TEXT("</string><string><![CDATA[ ") << data << TEXT(" ]]></string></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring remove(int layer) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Delete\" returntype=\"xml\"><arguments><array><property id=\"0\"><number>") << layer << TEXT("</number></property></array></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring play(int layer) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Play\" returntype=\"xml\"><arguments><array><property id=\"0\"><number>") << layer << TEXT("</number></property></array></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring stop(int layer, unsigned int mixOutDuration)
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Stop\" returntype=\"xml\"><arguments><array><property id=\"0\"><number>") << layer << TEXT("</number></property></array><number>") << mixOutDuration << TEXT("</number></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring next(int layer)
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Next\" returntype=\"xml\"><arguments><array><property id=\"0\"><number>") << layer << TEXT("</number></property></array></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring update(int layer, const std::wstring& data) 
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"SetData\" returntype=\"xml\"><arguments><array><property id=\"0\"><number>") << layer << TEXT("</number></property></array><string><![CDATA[ ") << data << TEXT(" ]]></string></arguments></invoke>");
		return flashParam.str();
	}

	virtual std::wstring invoke(int layer, const std::wstring& label)
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Invoke\" returntype=\"xml\"><arguments><array><property id=\"0\"><number>") << layer << TEXT("</number></property></array><string>") << label << TEXT("</string></arguments></invoke>");
		return flashParam.str();
	}
};

struct flash_cg_proxy18 : public flash_cg_proxy17
{
	virtual std::wstring add(int layer, const std::wstring& templateName, bool playOnLoad, const std::wstring& label, const std::wstring& data)
	{
		std::wstringstream flashParam;
		flashParam << TEXT("<invoke name=\"Add\" returntype=\"xml\"><arguments><number>") << layer << TEXT("</number><string>") << templateName << TEXT("</string>") << (playOnLoad?TEXT("<true/>"):TEXT("<false/>")) << TEXT("<string>") << label << TEXT("</string><string><![CDATA[ ") << data << TEXT(" ]]></string></arguments></invoke>");
		return flashParam.str();
	}
};

struct cg_producer::implementation : boost::noncopyable
{
public:

	implementation()
	{
		if(boost::filesystem::exists(server::template_folder()+TEXT("cg.fth.18")))
		{
			flash_producer_ = std::make_shared<flash_producer>(server::template_folder()+TEXT("cg.fth.18"));
			proxy_.reset(new flash_cg_proxy18());
			CASPAR_LOG(info) << L"Running version 1.8 template graphics.";
		}
		else if(boost::filesystem::exists(server::template_folder()+TEXT("cg.fth.17")))
		{
			flash_producer_ = std::make_shared<flash_producer>(server::template_folder()+TEXT("cg.fth.17"));
			proxy_.reset(new flash_cg_proxy17());
			CASPAR_LOG(info) << L"Running version 1.7 template graphics.";
		}
		else if(boost::filesystem::exists(server::template_folder()+TEXT("cg.fth"))) 
		{
			flash_producer_ = std::make_shared<flash_producer>(server::template_folder()+TEXT("cg.fth"));
			proxy_.reset(new flash_cg_proxy16());
			CASPAR_LOG(info) << L"Running version 1.6 template graphics.";
		}
		else 
			CASPAR_LOG(info) << L"No templatehost found. Template graphics will be disabled";
		
	}

	void clear()
	{
		flash_producer_.reset();
	}

	void add(int layer, const std::wstring& templateName,  bool playOnLoad, const std::wstring& startFromLabel, const std::wstring& data)
	{
		if(flash_producer_ == nullptr)
			return;
		CASPAR_LOG(info) << "Invoking add-command";
		flash_producer_->param(proxy_->add(layer, templateName, playOnLoad, startFromLabel, data));
	}

	void remove(int layer)
	{
		if(flash_producer_ == nullptr)
			return;
		CASPAR_LOG(info) << "Invoking remove-command";
		flash_producer_->param(proxy_->remove(layer));
	}

	void play(int layer)
	{
		if(flash_producer_ == nullptr)
			return;
		CASPAR_LOG(info) << "Invoking play-command";
		flash_producer_->param(proxy_->play(layer));
	}

	void stop(int layer, unsigned int mixOutDuration)
	{
		if(flash_producer_ == nullptr)
			return;
		CASPAR_LOG(info) << "Invoking stop-command";
		flash_producer_->param(proxy_->stop(layer, mixOutDuration));
	}

	void next(int layer)
	{
		if(flash_producer_ == nullptr)
			return;
		CASPAR_LOG(info) << "Invoking next-command";
		flash_producer_->param(proxy_->next(layer));
	}

	void update(int layer, const std::wstring& data)
	{
		if(flash_producer_ == nullptr)
			return;
		CASPAR_LOG(info) << "Invoking update-command";
		flash_producer_->param(proxy_->update(layer, data));
	}

	void invoke(int layer, const std::wstring& label)
	{
		if(flash_producer_ == nullptr)
			return;
		CASPAR_LOG(info) << "Invoking invoke-command";
		flash_producer_->param(proxy_->invoke(layer, label));
	}

	frame_ptr render_frame()
	{
		return flash_producer_ ? flash_producer_->render_frame() : nullptr;
	}
		
	void initialize(const frame_processor_device_ptr& frame_processor)
	{
		frame_processor_ = frame_processor;
		if(flash_producer_)
			flash_producer_->initialize(frame_processor_);
	}

	flash_producer_ptr flash_producer_;
	std::unique_ptr<flash_cg_proxy> proxy_;
	frame_processor_device_ptr frame_processor_;
};
	
// This is somewhat a hack... needs redesign
cg_producer_ptr get_default_cg_producer(const channel_ptr& channel, unsigned int render_layer)
{
	if(!channel)
		BOOST_THROW_EXCEPTION(null_argument() << msg_info("channel"));
	
	auto producer = std::dynamic_pointer_cast<cg_producer>(channel->active(render_layer));
	if(!producer)
	{
		producer = std::make_shared<cg_producer>();		
		channel->load(render_layer, producer, load_option::auto_play); 
	}
	
	return producer;
}

cg_producer::cg_producer() : impl_(new implementation()){}
frame_ptr cg_producer::render_frame(){return impl_->render_frame();}
void cg_producer::clear(){impl_->clear();}
void cg_producer::add(int layer, const std::wstring& templateName,  bool playOnLoad, const std::wstring& startFromLabel, const std::wstring& data){impl_->add(layer, templateName, playOnLoad, startFromLabel, data);}
void cg_producer::remove(int layer){impl_->remove(layer);}
void cg_producer::play(int layer){impl_->play(layer);}
void cg_producer::stop(int layer, unsigned int mixOutDuration){impl_->stop(layer, mixOutDuration);}
void cg_producer::next(int layer){impl_->next(layer);}
void cg_producer::update(int layer, const std::wstring& data){impl_->update(layer, data);}
void cg_producer::invoke(int layer, const std::wstring& label){impl_->invoke(layer, label);}
void cg_producer::initialize(const frame_processor_device_ptr& frame_processor){impl_->initialize(frame_processor);}
}}}