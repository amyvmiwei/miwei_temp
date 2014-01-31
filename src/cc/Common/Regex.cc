/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Definitions for Regex.
/// This file contains definitions for Regex, a static class containing regular
/// expression utility functions.

#include <Common/Compat.h>
#include "Regex.h"

using namespace Hypertable;

bool
Regex::extract_prefix(const char *regex, size_t regex_len,
                      const char **output, size_t *output_len,
                      DynamicBuffer &buf) {
  const char *ptr;
  if (regex_len == 0 || *regex != '^')
    return false;
  *output = regex+1;
  for (ptr=*output; ptr<regex+regex_len; ptr++) {
    if (*ptr == '.' || *ptr == '[' || *ptr == '(' || *ptr == '{' || *ptr == '\\' || *ptr == '+' || *ptr == '$') {
      *output_len = ptr - *output;
      return *output_len > 0;
    }
    else if (*ptr == '*' || *ptr == '?' || *ptr == '|') {
      if ((ptr - *output) == 0)
        return false;
      *output_len = (ptr - *output) - 1;
      return *output_len > 0;
    }
  }
  *output_len = ptr - *output;
  return *output_len > 0;
}
