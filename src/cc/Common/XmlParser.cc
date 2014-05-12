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
/// Definitions for XmlParser.
/// This file contains type definitions for XmlParser, an abstract base class
/// for XML document parsers.

#include <Common/Compat.h>
#include "XmlParser.h"

#include <Common/Error.h>
#include <Common/Logger.h>

#include <boost/algorithm/string.hpp>

#include <cctype>
#include <cstdlib>
#include <iostream>
#include <limits>

using namespace Hypertable;
using namespace std;

XmlParser::XmlParser(const char *base, int len) : m_base(base), m_length(len) {
  m_parser = XML_ParserCreate("US-ASCII");
  XML_SetElementHandler(m_parser, &start_element_handler, &end_element_handler);
  XML_SetCharacterDataHandler(m_parser, &character_data_handler);
  XML_SetUserData(m_parser, this);
}

XmlParser::XmlParser(const char *base, int len,
                     const initializer_list<std::string> &sub_parsers) :
  XmlParser(base, len) {
  m_sub_parsers = sub_parsers;
}


XmlParser::~XmlParser() {
  XML_ParserFree(m_parser);
}

void XmlParser::parse() {

  try {
    if (XML_Parse(m_parser, m_base, m_length, 1) == 0) {
      string msg;
      if (XML_GetErrorCode(m_parser) == XML_ERROR_TAG_MISMATCH)
        msg = format("%s (%s)",
                     (const char *)XML_ErrorString(XML_GetErrorCode(m_parser)),
                     m_current_element.c_str());
      else {
        const char *base = m_base + XML_GetCurrentByteIndex(m_parser);
        const char *end = base;
        for (; *end; ++end)
          if (isspace(*end))
            break;
        msg = format("%s (%s)",
                     (const char *)XML_ErrorString(XML_GetErrorCode(m_parser)),
                     string(base, end-base).c_str());
      }
      HT_THROW(Error::SCHEMA_PARSE_ERROR, msg);
    }
  }
  catch (Exception &e) {
    if (e.code() != Error::SCHEMA_PARSE_ERROR)
      HT_THROW(Error::SCHEMA_PARSE_ERROR, e.what());
    throw;
  }
}

int64_t XmlParser::content_to_i64(const std::string &name,
                                  const std::string &content) {
  if (content.empty())
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Empty content for %s", name.c_str());
  char *end;
  errno = 0;
  int64_t val = strtoll(content.c_str(), &end, 10);
  if (errno || *end)
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Invalid value for %s (%s)", name.c_str(), content.c_str());
  return val;
}

int32_t XmlParser::content_to_i32(const std::string &name,
                                  const std::string &content) {
  int64_t val64 = content_to_i64(name, content);
  if (val64 > (int64_t)numeric_limits<int32_t>::max() ||
      val64 < (int64_t)numeric_limits<int32_t>::min())
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Invalid value for %s (%s)", name.c_str(), content.c_str());
  return (int32_t)val64;
}

int16_t XmlParser::content_to_i16(const std::string &name,
                                  const std::string &content) {
  int64_t val64 = content_to_i64(name, content);
  if (val64 > (int64_t)numeric_limits<int16_t>::max() ||
      val64 < (int64_t)numeric_limits<int16_t>::min())
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Invalid value for %s (%s)", name.c_str(), content.c_str());
  return (int16_t)val64;
}

bool XmlParser::content_to_bool(const std::string &name,
                                const std::string &content) {
  if (content.empty())
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Empty content for %s", name.c_str());
  if (!strcasecmp(content.c_str(), "true"))
    return true;
  else if (!strcasecmp(content.c_str(), "false"))
    return false;
  HT_THROWF(Error::SCHEMA_PARSE_ERROR,
            "Invalid boolean value for %s (%s)", name.c_str(), content.c_str());
}

const std::string XmlParser::content_to_text(const std::string &name,
                                             const std::string &content,
                                const std::initializer_list<std::string> &valid) {
  if (content.empty())
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Empty content for %s", name.c_str());
  for (auto &v : valid) {
    if (!strcasecmp(v.c_str(), content.c_str()))
      return v;
  }
  HT_THROWF(Error::SCHEMA_PARSE_ERROR,
            "Invalid value for %s (%s)", name.c_str(), content.c_str());
}


bool XmlParser::open_element(const XML_Char *name) {
  if (m_sub_parse_toplevel >= 0) {
    HT_ASSERT(m_sub_parse_toplevel < (int)m_element_stack.size());
    push_element(name);
    return false;
  }
  for (auto &sp : m_sub_parsers) {
    if (!strcasecmp(sp.c_str(), name)) {
      m_sub_parse_base_offset = XML_GetCurrentByteIndex(m_parser);
      m_sub_parse_toplevel = m_element_stack.size();
      push_element(name);
      return false;
    }
  }
  return true;
}

bool XmlParser::close_element(const XML_Char *name) {
  if (m_sub_parse_toplevel > 0) {
    if (m_sub_parse_toplevel == (int)m_element_stack.size()) {
      int offset = XML_GetCurrentByteIndex(m_parser);
      for (; offset < m_length; ++offset) {
        if (m_base[offset] == '>') {
          offset++;
          sub_parse(name, &m_base[m_sub_parse_base_offset],
                    offset - m_sub_parse_base_offset);
          m_sub_parse_toplevel = -1;
          return false;
        }
      }
      HT_THROWF(Error::SCHEMA_PARSE_ERROR,
                "Unable to find '>' in close tag '%s'", name);
    }
    return false;
  }
  return true;
}

void XmlParser::add_text(const XML_Char *s, int len) {
  m_collected_text.assign(s, len);
}

void XmlParser::push_element(const XML_Char *name) {
  m_element_stack.push(name);
  m_current_element.clear();
  m_current_element.append(name);
  m_collected_text.clear();
}

void XmlParser::pop_element() {
  m_element_stack.pop();
  m_current_element.clear();
  if (!m_element_stack.empty())
    m_current_element.append(m_element_stack.top());
}

void XmlParser::start_element_handler(void *userdata, const XML_Char *name,
                                      const XML_Char **atts) {
  XmlParser *parser = reinterpret_cast<XmlParser *>(userdata);
  if (!parser->open_element(name))
    return;
  parser->start_element(name, atts);
  parser->push_element(name);
}

void XmlParser::end_element_handler(void *userdata, const XML_Char *name) {
  XmlParser *parser = reinterpret_cast<XmlParser *>(userdata);
  parser->pop_element();
  if (!parser->close_element(name))
    return;
  string content = parser->collected_text();
  boost::trim(content);  
  parser->end_element(name, content);
}

void XmlParser::character_data_handler(void *userdata, const XML_Char *s, int len) {
  XmlParser *parser = reinterpret_cast<XmlParser *>(userdata);
  parser->add_text(s, len);
}


