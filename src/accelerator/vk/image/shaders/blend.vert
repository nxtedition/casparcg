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
 * Author: CasparCG Team
 *
 * Full-screen triangle vertex shader for blend operations.
 * Uses a single triangle that covers the entire viewport.
 */

#version 450

// Output texture coordinates
layout(location = 0) out vec2 out_tex_coord;

void main()
{
    // Generate full-screen triangle vertices
    // Vertex 0: (-1, -1) -> tex (0, 0)
    // Vertex 1: ( 3, -1) -> tex (2, 0)
    // Vertex 2: (-1,  3) -> tex (0, 2)
    // This creates a triangle that covers the entire screen with proper UV mapping

    vec2 positions[3] = vec2[](
        vec2(-1.0, -1.0),
        vec2( 3.0, -1.0),
        vec2(-1.0,  3.0)
    );

    vec2 tex_coords[3] = vec2[](
        vec2(0.0, 0.0),
        vec2(2.0, 0.0),
        vec2(0.0, 2.0)
    );

    gl_Position = vec4(positions[gl_VertexIndex], 0.0, 1.0);
    out_tex_coord = tex_coords[gl_VertexIndex];
}
