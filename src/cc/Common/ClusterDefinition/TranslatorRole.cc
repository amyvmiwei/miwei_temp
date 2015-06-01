/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
/// Definitions for TranslatorRole.
/// This file contains type definitions for TranslatorRole, a class for
/// translating a role definition statement.

#include <Common/Compat.h>

#include "TranslatorRole.h"

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/String.h>

#include <boost/algorithm/string.hpp>

#include <cctype>

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace std;

const string TranslatorRole::translate(TranslationContext &context) {
  string translated_text;
  string translated_value;
  size_t offset = m_text.find_first_of("role:");
  if (offset == string::npos)
    HT_THROWF(Error::SYNTAX_ERROR, "Bad role definition on line %d of '%s'",
              (int)m_lineno, m_fname.c_str());
  const char *ptr = m_text.c_str() + offset + 5;
  // skip whitespace
  while (*ptr && isspace(*ptr))
    ptr++;

  const char *base = ptr;
  if (*ptr == 0 || !isalpha(*ptr))
    HT_THROWF(Error::SYNTAX_ERROR, "Bad role definition on line %d of '%s'",
              (int)m_lineno, m_fname.c_str());
  while (*ptr && (isalnum(*ptr) || *ptr == '_'))
    ptr++;

  string name(base, ptr-base);

  if (name.compare("all") == 0)
    HT_THROWF(Error::SYNTAX_ERROR, "Invalid role name 'all' on line %d of '%s'",
              (int)m_lineno, m_fname.c_str());

  if (*ptr == 0 || !isspace(*ptr))
    HT_THROWF(Error::SYNTAX_ERROR,
              "Invalid role specification for '%s' on line %d of '%s'",
              name.c_str(), (int)m_lineno, m_fname.c_str());

  while (*ptr && isspace(*ptr))
    ptr++;

  base = 0;
  while (*ptr) {
    if (base == 0 && *ptr == '-') {
      translated_value.append(1, *ptr);
    }
    else if (isalnum(*ptr) || *ptr == '_' || *ptr == '-' || *ptr == '.') {
      if (base == 0)
        base = ptr;
    }
    else if (isspace(*ptr) || *ptr == '+' ||
             *ptr == ',' || *ptr == '(' || *ptr == ')') {
      if (base) {
        string name(base, ptr-base);
        if (context.roles.count(name) > 0) {
          translated_value.append("${ROLE_");
          translated_value.append(name);
          translated_value.append("}");
        }
        else
          translated_value.append(name);
        base = 0;
      }
      if (isspace(*ptr)) {
        if (!isspace(translated_value[translated_value.length()-1]))
          translated_value.append(1, ' ');
      }
      else
        translated_value.append(1, *ptr);
    }
    else if (*ptr == '[' || *ptr == ']') {
      if (base) {
        translated_value.append(base, ptr-base);
        base = 0;
      }
      translated_value.append(1, *ptr);      
    }
    else
      HT_THROWF(Error::SYNTAX_ERROR, "Invalid character '%c' found in role "
                "definition on line %d of '%s'",
                *ptr, (int)m_lineno, m_fname.c_str());
    ptr++;
  }

  boost::trim_right_if(translated_value, boost::is_any_of(" \t\n\r"));

  translated_text.append("ROLE_");
  translated_text.append(name);
  translated_text.append("=\"");
  translated_text.append(translated_value);
  translated_text.append("\"\n");

  context.roles.insert(name);
  context.symbols["ROLE_"+name] = translated_value;

  return translated_text;
}

