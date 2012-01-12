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

#pragma once

#include "../../video_format.h"

#include <common/memory/safe_ptr.h>
#include <common/enum_class.h>
#include <common/utility/tweener.h>

#include <string>

namespace caspar { namespace core {
	
struct transition_type_def
{
	enum type
	{
		cut,	
		mix,	
		push,	 
		slide,	
		wipe,
		count
	};
};
typedef enum_class<transition_type_def> transition_type;
	
struct transition_direction_def
{
	enum type
	{
		from_left,
		from_right,
		count
	};
};
typedef enum_class<transition_direction_def> transition_direction;

struct transition_info
{
	transition_info() 
		: type(transition_type::cut)
		, duration(0)
		, direction(transition_direction::from_left)
		, tweener(get_tweener(L"linear")){}
		
	size_t					duration;
	transition_direction	direction;
	transition_type			type;
	tweener_t				tweener;
};

safe_ptr<struct frame_producer> create_transition_producer(const field_mode& mode, const safe_ptr<struct frame_producer>& destination, const transition_info& info);

}}