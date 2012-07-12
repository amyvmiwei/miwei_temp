/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
#include "Compat.h"

#include "Debug.h"

#include <boost/shared_array.hpp>

using namespace Hypertable;

void Debug::binary_to_human_readable(const uint8_t *binary, size_t binary_len, std::string &str) {
  boost::shared_array<char> buf( new char[(binary_len*7)+1] );
  char *ptr = buf.get();
  int hexdigit;

  for (size_t i=0; i<binary_len; i++) {
    hexdigit = (binary[i] & 0xF0) >> 4;
    if (hexdigit < 10)
      *ptr++ = hexdigit + '0';
    else
      *ptr++ = (hexdigit-10) + 'A';
    hexdigit = (binary[i] & 0x0F);
    if (hexdigit < 10)
      *ptr++ = hexdigit + '0';
    else
      *ptr++ = (hexdigit-10) + 'A';
    if (isprint((char)binary[i])) {
      *ptr++ = ' ';
      *ptr++ = '\'';
      *ptr++ = (char)binary[i];
      *ptr++ = '\'';
    }
    *ptr++ = '\n';
  }
  *ptr = 0;
  str = std::string(buf.get());
}
