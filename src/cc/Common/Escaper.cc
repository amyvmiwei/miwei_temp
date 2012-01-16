/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
#include "Common/Compat.h"

#include <boost/shared_array.hpp>

#include "Escaper.h"

using namespace Hypertable;

void Hypertable::escape(String &str, const String &escape_chars) {
  if (str.length() > 0 && escape_chars.length() > 0) {
    bool escaped[256];
    boost::shared_array<char> escaped_str(new char [(2*str.length()) + 1]);
    char *dst = escaped_str.get();

    memset(escaped, 0, 256*sizeof(bool));
    for (size_t i=0; i<escape_chars.length(); i++)
      escaped[(size_t)escape_chars[i]] = true;
    escaped[(size_t)'\\'] = true;

    const char *src = str.c_str();

    while (*src) {
      if (escaped[(size_t)*src])
        *dst++ = '\\';
      *dst++ = *src++;
    }
    *dst = 0;
    str = String(escaped_str.get());
  }
  
}

void Hypertable::unescape(String &str) {
  if (str.length() > 0) {
    boost::shared_array<char> escaped_str(new char [str.length() + 1]);
    char *dst = escaped_str.get();
    for (const char *src = str.c_str(); *src; src++) {
      if (*src == '\\' && *(src+1) == '\\')
        *dst++ = *src++;
      else if (*src != '\\')
        *dst++ = *src;
    }
    *dst = 0;
    str = String(escaped_str.get());
  }
}
