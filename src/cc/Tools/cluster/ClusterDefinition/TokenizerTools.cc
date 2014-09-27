/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Definitions for TokenizerTools.
/// This file contains type definitions for TokenizerTools, a namespace
/// containing utility functions to assist in cluster definition file
/// tokenization.

#include <Common/Compat.h>

#include "TokenizerTools.h"

#include <Common/Logger.h>

#include <cctype>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <stack>

using namespace std;

namespace Hypertable { namespace ClusterDefinition { namespace TokenizerTools {

bool is_identifier_start_character(char c) {
  return isalpha(c) || c == '_';
}

bool is_identifier_character(char c) {
  return is_identifier_start_character(c) || isdigit(c);
}

bool is_valid_identifier(const string &name) {
  const char *ptr = name.c_str();
  if (*ptr == 0 || !is_identifier_start_character(*ptr))
    return false;
  for (ptr=ptr+1; *ptr; ptr++) {
    if (!is_identifier_character(*ptr))
      return false;
  }
  return true;
}

bool is_number(const string &str) {
  char *end;

  errno = 0;
  long val = strtol(str.c_str(), &end, 10);

  if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
      || (errno != 0 && val == 0))
    return false;

  if (end == 0 || *end)
    return false;

  return true;
}

bool find_token(const string &token, const char *base, const char *end, size_t *offsetp) {
  char quote_char = 0;
  const char *ptr = base;
  HT_ASSERT(!token.empty());
  end -= token.length() - 1;
  while (ptr < end) {
    if (quote_char == 0) {
      if (*ptr == '"' || *ptr == '\'' || *ptr == '`')
        quote_char = *ptr;
      else if (strncmp(ptr, token.c_str(), token.length()) == 0) {
        *offsetp = ptr-base;
        return true;
      }
    }
    else {
      if (*ptr == quote_char && *(ptr-1) != '\\')
        quote_char = 0;
    }
    ptr++;
  }
  return false;
}



bool find_end_char(const char *base, const char **endp, size_t *linep) {
  stack<char> scope;

  HT_ASSERT(*base == '"' || *base == '\'' || *base == '`' || *base == '{');

  scope.push(*base);
  const char *ptr = base+1;

  while (*ptr) {

    if (scope.top() == '"') {
      if (*ptr == '"' && *(ptr-1) != '\\') {
        scope.pop();
        if (scope.empty())
          break;
      }
    }
    else if (scope.top() == '\'') {
      if (*ptr == '\'' && *(ptr-1) != '\\') {
        scope.pop();
        if (scope.empty())
          break;
      }
    }
    else if (scope.top() == '`') {
      if (*ptr == '`') {
        scope.pop();
        if (scope.empty())
          break;
      }
    }
    else {
      HT_ASSERT(scope.top() == '{');
      if (*ptr == '}') {
        scope.pop();
        if (scope.empty())
          break;
      }
      else if (*ptr == '"' || *ptr == '\'' || *ptr == '`' || *ptr == '{')
        scope.push(*ptr);
    }
    if (*ptr == '\n' && linep)
      (*linep)++;
    ptr++;
  }

  if (*ptr == 0)
    return false;

  *endp = ptr;
  return true;
}

size_t count_newlines(const char *base, const char *end) {
  size_t count = 0;
  for (const char *ptr=base; ptr<end; ptr++) {
    if (*ptr == '\n')
      count++;
  }
  return count;
}

bool skip_to_newline(const char **endp) {
  const char *ptr = *endp;
  while (*ptr && *ptr != '\n')
    ptr++;
  *endp = ptr;
  return (*ptr == '\n');
}

bool substitute_variables(const string &input, string &output,
                          map<string, string> &vmap) {
  bool ret {};
  string translated_text;
  const char *base = input.c_str();
  const char *ptr = strstr(base, "${");
  while (ptr) {
    translated_text.append(base, ptr-base);
    base = ptr;
    if ((ptr = strchr(base, '}')) != nullptr) {
      string variable(base+2, (ptr-base)-2);
      auto iter = vmap.find(variable);
      if (iter != vmap.end()) {
        translated_text.append(vmap[variable]);
        ret = true;
      }
      else
        translated_text.append(base, (ptr-base)+1);
      base = ptr + 1;
      ptr = strstr(base, "${");      
    }
  }
  translated_text.append(base);
  output.clear();
  output.append(translated_text);
  return ret;
}


}}}
