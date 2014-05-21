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
/// Definitions for ColumnFamilySpec.
/// This file contains type definitions for ColumnFamilySpec, a class representing
/// a column family specification.

#include <Common/Compat.h>
#include "ColumnFamilySpec.h"

#include <Common/Error.h>
#include <Common/XmlParser.h>

#include <algorithm>
#include <stack>

using namespace Hypertable;
using namespace std;

bool ColumnFamilyOptions::set_max_versions(int32_t max_versions) {

  if (max_versions < 0)
    HT_THROWF(Error::BAD_VALUE,
              "Invalid value for MAX VERSIONS (%d), must be non-negative",
              (int)max_versions);
    
  if (m_counter && max_versions != 0)
    HT_THROW(Error::INCOMPATIBLE_OPTIONS,
              "COUNTER and MAX VERSIONS are incompatible");

  m_isset.set(MAX_VERSIONS);

  if (m_max_versions != max_versions) {
    m_max_versions = max_versions;
    return true;
  }
  return false;
}

bool ColumnFamilyOptions::is_set_max_versions() const {
  return m_isset.test(MAX_VERSIONS);
}


bool ColumnFamilyOptions::set_ttl(time_t ttl) {
  if (ttl < 0)
    HT_THROWF(Error::BAD_VALUE,
              "Invalid value for TTL (%d), must be non-negative", (int)ttl);
  m_isset.set(TTL);
  if (m_ttl != ttl) {
    m_ttl = ttl;
    return true;
  }
  return false;
}

bool ColumnFamilyOptions::is_set_ttl() const {
  return m_isset.test(TTL);
}


bool ColumnFamilyOptions::set_time_order_desc(bool value) {
  if (value) {
    if (m_counter)
      HT_THROW(Error::INCOMPATIBLE_OPTIONS,
               "COUNTER and TIME_ORDER_DESC are incompatible");
  }
  m_isset.set(TIME_ORDER_DESC);
  if (m_time_order_desc != value) {
    m_time_order_desc = value;
    return true;
  }
  return false;
}


bool ColumnFamilyOptions::is_set_time_order_desc() const {
  return m_isset.test(TIME_ORDER_DESC);
}


bool ColumnFamilyOptions::set_counter(bool value) {
  if (value) {
    if (m_max_versions)
      HT_THROW(Error::INCOMPATIBLE_OPTIONS,
                "COUNTER and MAX_VERSIONS are incompatible");
    if (m_time_order_desc)
      HT_THROW(Error::INCOMPATIBLE_OPTIONS,
                "COUNTER and TIME_ORDER_DESC are incompatible");
  }
  m_isset.set(COUNTER);
  if (m_counter != value) {
    m_counter = value;
    return true;
  }
  return false;
}

bool ColumnFamilyOptions::is_set_counter() const {
  return m_isset.test(COUNTER);
}


void ColumnFamilyOptions::merge(const ColumnFamilyOptions &other) {
  if (!is_set_max_versions() && other.is_set_max_versions())
    set_max_versions(other.get_max_versions());
  if (!is_set_ttl() && other.is_set_ttl())
    set_ttl(other.get_ttl());
  if (!is_set_time_order_desc() && other.is_set_time_order_desc())
    set_time_order_desc(other.get_time_order_desc());
  if (!is_set_counter() && other.is_set_counter())
    set_counter(other.get_counter());
}

namespace {
  
  class XmlParserColumnFamilyOptions : public XmlParser {
  public:
    XmlParserColumnFamilyOptions(ColumnFamilyOptions *ops, const char *s,
                                 int len) : XmlParser(s,len), m_options(ops) {}

    void end_element(const XML_Char *name, const string &content) override {
      if (!strcasecmp(name, "MaxVersions"))
        m_options->set_max_versions(content_to_i32(name, content));
      else if (!strcasecmp(name, "TTL"))
        m_options->set_ttl((time_t)content_to_i32(name, content));
      else if (!strcasecmp(name, "TimeOrder"))
        m_options->set_time_order_desc(content_to_text(name, content,
                                                       {"asc","desc"})=="desc");
      else if (!strcasecmp(name, "Counter"))
        m_options->set_counter(content_to_bool(name, content));
      else if (!m_element_stack.empty())
        HT_THROWF(Error::SCHEMA_PARSE_ERROR,
                  "Unrecognized ColumnFamily option element (%s)", name);
    }
  private:
    ColumnFamilyOptions *m_options;
  };
  
}

void ColumnFamilyOptions::parse_xml(const char *base, int len) {
  XmlParserColumnFamilyOptions parser(this, base, len);
  parser.parse();
}

const std::string ColumnFamilyOptions::render_xml(const std::string &line_prefix) const {
  std::string xstr;
  if (is_set_max_versions())
    xstr += format("%s<MaxVersions>%d</MaxVersions>\n",
                   line_prefix.c_str(), (int)m_max_versions);
  if (is_set_ttl())
    xstr += format("%s<TTL>%d</TTL>\n", line_prefix.c_str(), (int)m_ttl);
  if (is_set_time_order_desc())
    xstr += format("%s<TimeOrder>%s</TimeOrder>\n", line_prefix.c_str(),
                   m_time_order_desc ? "desc" : "asc");
  if (is_set_counter())
    xstr += format("%s<Counter>%s</Counter>\n", line_prefix.c_str(),
                   m_counter ? "true" : "false");
  return xstr;
}

const std::string ColumnFamilyOptions::render_hql() const {
  std::string hstr;
  if (is_set_max_versions())
    hstr += format(" MAX_VERSIONS %d", (int)m_max_versions);
  if (is_set_ttl())
    hstr += format(" TTL %d", (int)m_ttl);
  if (is_set_time_order_desc())
    hstr += format(" TIME_ORDER %s", m_time_order_desc ? "desc" : "asc");
  if (is_set_counter())
    hstr += format(" COUNTER %s", m_counter ? "true" : "false");
  return hstr;
}

bool ColumnFamilyOptions::operator==(const ColumnFamilyOptions &other) const {
  return (m_isset == other.m_isset &&
          m_max_versions == other.m_max_versions &&
          m_ttl == other.m_ttl &&
          m_time_order_desc == other.m_time_order_desc &&
          m_counter == other.m_counter);
}

void ColumnFamilySpec::set_name(const std::string &name) {
  m_name = name;
}

void ColumnFamilySpec::set_access_group(const std::string &ag) {
  m_ag = ag;
}

void ColumnFamilySpec::set_generation(int64_t generation) {
  m_generation = generation;
}

void ColumnFamilySpec::set_id(int32_t id) {
  m_id = id;
}

void ColumnFamilySpec::set_deleted(bool value) {
  if (m_deleted != value) {
    m_deleted = value;
    m_generation = 0;
  }
}

void ColumnFamilySpec::set_option_max_versions(int32_t max_versions) {
  if (m_options.set_max_versions(max_versions))
    m_generation = 0;
}

void ColumnFamilySpec::set_option_ttl(time_t ttl) {
  if (m_options.set_ttl(ttl))
    m_generation = 0;
}

void ColumnFamilySpec::set_option_time_order_desc(bool value) {
  if (m_options.set_time_order_desc(value))
    m_generation = 0;
}

void ColumnFamilySpec::set_option_counter(bool value) {

  if (value) {
    if (m_value_index)
      HT_THROW(Error::INCOMPATIBLE_OPTIONS,
                "COUNTER and INDEX are incompatible");
    if (m_qualifier_index)
      HT_THROW(Error::INCOMPATIBLE_OPTIONS,
               "COUNTER and QUALIFIER INDEX are incompatible");
  }

  if (m_options.set_counter(value))
    m_generation = 0;
}

void ColumnFamilySpec::set_value_index(bool value) {
  if (value && m_options.get_counter())
    HT_THROW(Error::INCOMPATIBLE_OPTIONS,
             "COUNTER and INDEX are incompatible");
  if (m_value_index != value) {
    m_value_index = value;
    m_generation = 0;
  }
}

void ColumnFamilySpec::set_qualifier_index(bool value) {
  if (value && m_options.get_counter())
    HT_THROW(Error::INCOMPATIBLE_OPTIONS,
             "COUNTER and QUALIFIER INDEX are incompatible");
  if (m_qualifier_index != value) {
    m_qualifier_index = value;
    m_generation = 0;
  }
}

void ColumnFamilySpec::merge_options(const ColumnFamilyOptions &other) {
  m_options.merge(other);
}

bool ColumnFamilySpec::operator==(const ColumnFamilySpec &other) const {
  return (m_name == other.m_name &&
          m_ag == other.m_ag &&
          m_generation == other.m_generation &&
          m_id == other.m_id &&
          m_options == other.m_options &&
          m_value_index == other.m_value_index &&
          m_qualifier_index == other.m_qualifier_index &&
          m_deleted == other.m_deleted);
}


namespace {
  
  class XmlParserColumnFamilySpec : public XmlParser {
  public:
    XmlParserColumnFamilySpec(ColumnFamilySpec *spec, const char *s, int len) :
      XmlParser(s,len,{"Options"}), m_spec(spec) {}

    void start_element(const XML_Char *name, const XML_Char **atts) override {
      if (m_element_stack.empty()) {
        for (int i=0; atts[i] != 0; i+=2) {
          if (!strcasecmp(atts[i], "id"))
            m_spec->set_id(content_to_i32("id", atts[i+1]));
          else
            HT_THROWF(Error::SCHEMA_PARSE_ERROR,
                      "Unrecognized attribute (%s) in ColumnFamily element", atts[i]);
        }
      }
    }

    void end_element(const XML_Char *name, const string &content) override {
      if (!strcasecmp(name, "Name"))
        m_spec->set_name(content);
      else if (!strcasecmp(name, "AccessGroup"))
        m_spec->set_access_group(content);
      else if (!strcasecmp(name, "Generation"))
        m_generation = content_to_i64(name, content);
      else if (!strcasecmp(name, "Deleted"))
        m_spec->set_deleted(content_to_bool(name, content));
      else if (!strcasecmp(name, "Index"))
        m_spec->set_value_index(content_to_bool(name, content));
      else if (!strcasecmp(name, "QualifierIndex"))
        m_spec->set_qualifier_index(content_to_bool(name, content));
      else if (m_element_stack.empty())
        m_spec->set_generation(m_generation);
      else {
        // backward compatibility
        if (!strcasecmp(name, "MaxVersions"))
          m_spec->set_option_max_versions(content_to_i32(name, content));
        else if (!strcasecmp(name, "TTL"))
          m_spec->set_option_ttl((time_t)content_to_i32(name, content));
        else if (!strcasecmp(name, "TimeOrder")) {
          bool val = content_to_text(name, content, {"asc","desc"}) == "desc";
          m_spec->set_option_time_order_desc(val);
        }
        else if (!strcasecmp(name, "Counter"))
          m_spec->set_option_counter(content_to_bool(name, content));
        else if (!strcasecmp(name, "Renamed") || !strcasecmp(name, "NewName"))
          ;
        else
          HT_THROWF(Error::SCHEMA_PARSE_ERROR,
                    "Unrecognized ColumnFamily element (%s)", name);
      }
    }

    void sub_parse(const XML_Char *name, const char *s, int len) override {
      ColumnFamilyOptions options;
      XmlParserColumnFamilyOptions parser(&options, s, len);
      parser.parse();
      m_spec->merge_options(options);
    }

  private:
    ColumnFamilySpec *m_spec;
    int64_t m_generation {};
  };
  
}

void ColumnFamilySpec::parse_xml(const char *base, int len) {
  XmlParserColumnFamilySpec parser(this, base, len);
  parser.parse();
}

const std::string ColumnFamilySpec::render_xml(const std::string &line_prefix,
                                               bool with_ids) const {
  string output;
  stack<string> prefix;
  prefix.push(line_prefix);
  output += prefix.top() + "<ColumnFamily";
  prefix.push(prefix.top() + "  ");
  if (with_ids) {
    output += format(" id=\"%u\"", get_id());
    output += ">\n";
    output += prefix.top() + format("<Generation>%lld</Generation>\n", (Lld)get_generation());
  }
  else
    output += ">\n";
  output += prefix.top() + format("<Name>%s</Name>\n", get_name().c_str());
  output += prefix.top() + format("<AccessGroup>%s</AccessGroup>\n", get_access_group().c_str());
  output += prefix.top() + format("<Deleted>%s</Deleted>\n",
                                   get_deleted() ? "true" : "false");
  if (get_value_index())
    output += prefix.top() + format("<Index>true</Index>\n");
  if (get_qualifier_index())
    output += prefix.top() + format("<QualifierIndex>true</QualifierIndex>\n");
  output += prefix.top() + format("<Options>\n");
  prefix.push(prefix.top() + "  ");
  output += options().render_xml(prefix.top());
  prefix.pop();
  output += prefix.top() + format("</Options>\n");
  prefix.pop();
  output += prefix.top() + "</ColumnFamily>\n";
  return output;
}

namespace {
  const std::string maybe_quote(const string &name) {
    auto valid_char = [](char c) { return isalnum(c); };
    if (!isalpha(name[0]) ||
        find_if_not(name.begin(), name.end(), valid_char) != name.end())
      return String("'") + name + "'";
    return name;
  }
}

const std::string ColumnFamilySpec::render_hql() const {
  string output;
  // don't display deleted cfs
  if (get_deleted())
    return output;

  output += format("  %s", maybe_quote(get_name()).c_str());

  output += options().render_hql();

  if (get_value_index())
    output += format(", INDEX %s", maybe_quote(get_name()).c_str());

  if (get_qualifier_index())
    output += format(", QUALIFIER INDEX %s", maybe_quote(get_name()).c_str());

  return output;
}
