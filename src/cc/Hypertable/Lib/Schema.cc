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
/// Definitions for Schema.
/// This file contains type definitions for Schema, a class representing a
/// table schema specification.

#include <Common/Compat.h>
#include "Schema.h"

#include <Common/Config.h>
#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/StringExt.h>
#include <Common/System.h>
#include <Common/XmlParser.h>

#include <expat.h>

#include <boost/algorithm/string.hpp>

extern "C" {
#include <ctype.h>
#include <errno.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <string>

using namespace Hypertable;
using namespace Property;
using namespace std;

#define MAX_COLUMN_ID 255

namespace {
  const std::string maybe_quote(const string &name) {
    auto valid_char = [](char c) { return isalnum(c); };
    if (!isalpha(name[0]) ||
        find_if_not(name.begin(), name.end(), valid_char) != name.end())
      return String("'") + name + "'";
    return name;
  }
}

/**
 * Assumes src_schema has been checked for validity
 */
Schema::Schema(const Schema &other)
  : m_arena(1024)
{
  m_access_group_map = CstrAccessGroupMap(LtCstr(), CstrAlloc(m_arena));
  m_column_family_map = CstrColumnFamilyMap(LtCstr(), CstrAlloc(m_arena));

  // Set schema attributes
  m_generation = other.m_generation;
  m_version = other.m_version;
  m_group_commit_interval = other.m_group_commit_interval;

  // Create access groups
  for (auto src_ag : other.m_access_groups) {
    AccessGroupSpec *ag = new AccessGroupSpec(src_ag->get_name());

    if (src_ag->options().is_set_replication())
      ag->set_option_replication(src_ag->get_option_replication());
    if (src_ag->options().is_set_blocksize())
      ag->set_option_blocksize(src_ag->get_option_blocksize());
    if (src_ag->options().is_set_compressor())
      ag->set_option_compressor(src_ag->get_option_compressor());
    if (src_ag->options().is_set_bloom_filter())
      ag->set_option_bloom_filter(src_ag->get_option_bloom_filter());
    if (src_ag->options().is_set_in_memory())
      ag->set_option_in_memory(src_ag->get_option_in_memory());

    if (src_ag->defaults().is_set_max_versions())
      ag->set_default_max_versions(src_ag->get_default_max_versions());
    if (src_ag->defaults().is_set_ttl())
      ag->set_default_ttl(src_ag->get_default_ttl());
    if (src_ag->defaults().is_set_time_order_desc())
      ag->set_default_time_order_desc(src_ag->get_default_time_order_desc());
    if (src_ag->defaults().is_set_counter())
      ag->set_default_counter(src_ag->get_default_counter());

    ag->set_generation(src_ag->get_generation());

    // Populate access group with column families
    for (auto src_cf : src_ag->columns())
      ag->add_column(new ColumnFamilySpec(*src_cf));

    m_access_group_map.insert(make_pair(m_arena.dup(ag->get_name()), ag));
    m_access_groups.push_back(ag);

  }

  m_ag_defaults = other.m_ag_defaults;
  m_cf_defaults = other.m_cf_defaults;

  validate();

}

Schema::~Schema() {
  for_each(m_access_groups.begin(), m_access_groups.end(),
           [](AccessGroupSpec *ag) { delete ag; });
}


namespace {
  
  class XmlParserSchema : public XmlParser {
  public:
    XmlParserSchema(Schema *schema, const char *s, int len) :
      XmlParser(s,len,{"AccessGroup","AccessGroupDefaults","ColumnFamilyDefaults"}),
      m_schema(schema) {}

    void start_element(const XML_Char *name, const XML_Char **atts) override {
      if (m_element_stack.empty()) {
        for (int i=0; atts[i] != 0; i+=2) {
          if (!strcasecmp(atts[i], "generation"))
            m_schema->set_generation(content_to_i64(atts[i], atts[i+1]));
          else if (!strcasecmp(atts[i], "version"))
            m_schema->set_version(content_to_i32(atts[i], atts[i+1]));
          else if (!strcasecmp(atts[i], "group_commit_interval"))
            m_schema->set_group_commit_interval(content_to_i32(atts[i], atts[i+1]));
          else if (!strcasecmp(atts[i], "compressor"))
            m_schema->access_group_defaults().set_compressor(atts[i+1]);
          else
            HT_THROWF(Error::SCHEMA_PARSE_ERROR,
                      "Unrecognized attribute (%s) in Schema element", atts[i]);
        }
      }
      else if (strcasecmp(name, "Generation") &&
               strcasecmp(name, "GroupCommitInterval"))
        HT_THROWF(Error::SCHEMA_PARSE_ERROR,
                  "Unrecognized Schema element (%s)", name);
    }

    void end_element(const XML_Char *name, const string &content) override {
      if (!strcasecmp(name, "Generation"))
        m_schema->set_generation(content_to_i64(name, content));
      else if (!strcasecmp(name, "GroupCommitInterval"))
        m_schema->set_group_commit_interval(content_to_i32(name, content));
      else if (!m_element_stack.empty())
        HT_THROWF(Error::SCHEMA_PARSE_ERROR,
                  "Unrecognized Schema element (%s)", name);
    }

    void sub_parse(const XML_Char *name, const char *s, int len) override {
      if (!strcasecmp(name, "AccessGroup")) {
        AccessGroupSpec *ag_spec = new AccessGroupSpec();
        try { ag_spec->parse_xml(s, len); }
        catch (Exception &) { delete ag_spec; throw; }
        m_schema->add_access_group(ag_spec);
      }
      else if (!strcasecmp(name, "AccessGroupDefaults")) {
        AccessGroupOptions defaults;
        defaults.parse_xml(s, len);
        m_schema->set_access_group_defaults(defaults);
      }
      else if (!strcasecmp(name, "ColumnFamilyDefaults")) {
        ColumnFamilyOptions defaults;
        defaults.parse_xml(s, len);
        m_schema->set_column_family_defaults(defaults);
      }
    }

  private:
    Schema *m_schema;
  };
  
}


Schema *Schema::new_instance(const string &buf) {
  Schema *schema = new Schema();
  XmlParserSchema parser(schema, buf.c_str(), buf.length());
  try {
    parser.parse();
    schema->validate();
  }
  catch (Exception &) {
    delete schema;
    throw;
  }
  return schema;
}

void Schema::clear_generation() {
  m_generation = 0;
  for (auto ag : m_access_groups)
    for (auto cf : ag->columns())
      cf->set_generation(0);
}

bool Schema::clear_generation_if_changed(Schema &original) {
  bool changed {};
  for (auto ag : m_access_groups) {
    auto orig_ag = original.get_access_group(ag->get_name());
    if (orig_ag == nullptr || ag->clear_generation_if_changed(*orig_ag)) {
      m_generation = 0;
      changed = true;
    }
  }
  if (!changed) {
    for (auto ag : original.get_access_groups()) {
      if (get_access_group(ag->get_name()) == nullptr) {
        m_generation = 0;
        changed = true;
        break;
      }
    }
  }
  return changed;
}

void Schema::update_generation(int64_t generation) {
  int64_t max_id = get_max_column_family_id();
  for (auto ag : m_access_groups) {
    for (auto cf : ag->columns()) {
      if (cf->get_generation() == 0) {
        if (cf->get_id() == 0)
          cf->set_id(++max_id);
        cf->set_generation(generation);
        ag->set_generation(generation);
        m_generation = generation;
      }
      else
        HT_ASSERT(cf->get_id());
    }
    if (ag->get_generation() == 0)
      ag->set_generation(generation);
  }
  validate();
}


const string Schema::render_xml(bool with_ids) {
  string output = "<Schema";
  if (m_version != 0)
    output += format(" version=\"%d\"", (int)m_version);
  output += ">\n";

  if (with_ids)
    output += format("  <Generation>%lld</Generation>\n", (Lld)m_generation);

  if (m_group_commit_interval > 0)
    output += format("  <GroupCommitInterval>%u</GroupCommitInterval>\n", m_group_commit_interval);

  output += "  <AccessGroupDefaults>\n";
  output += m_ag_defaults.render_xml("    ");
  output += "  </AccessGroupDefaults>\n";

  output += "  <ColumnFamilyDefaults>\n";
  output += m_cf_defaults.render_xml("    ");
  output += "  </ColumnFamilyDefaults>\n";

  for (auto ag_spec : m_access_groups)
    output += ag_spec->render_xml("  ", with_ids);

  output += "</Schema>\n";
  return output;
}

const string Schema::render_hql(const string &table_name) {
  string output = "CREATE TABLE ";
  output += maybe_quote(table_name);
  output += " (\n";

  for (auto ag_spec : m_access_groups) {
    for (auto cf_spec : ag_spec->columns()) {
      if (!cf_spec->get_deleted()) {
        output += cf_spec->render_hql();
        output += ",\n";
      }
    }
  }

  size_t i = 1;
  for (auto ag_spec : m_access_groups) {
    output += ag_spec->render_hql();
    output += i == m_access_groups.size() ? "\n" : ",\n";
    i++;
  }
  output += ")";

  if (m_group_commit_interval > 0)
    output += format(" GROUP_COMMIT_INTERVAL %u", m_group_commit_interval);

  output += m_ag_defaults.render_hql();
  output += m_cf_defaults.render_hql();
  return output;
}


/**
 * Protected methods
 */


int32_t Schema::get_max_column_family_id() {
  int32_t max {};
  for (auto ag : m_access_groups)
    for (auto cf : ag->columns())
      if (cf->get_id() > max)
        max = cf->get_id();
  return max;
}

TableParts Schema::get_table_parts() {
  int parts {};
  for (auto ag : m_access_groups) {
    for (auto cf : ag->columns()) {
      if (!cf->get_deleted()) {
        parts |= TableParts::PRIMARY;
        if (cf->get_value_index())
          parts |= TableParts::VALUE_INDEX;
        if (cf->get_qualifier_index())
          parts |= TableParts::QUALIFIER_INDEX;
      }
    }
  }
  return TableParts(parts);
}


void Schema::add_access_group(AccessGroupSpec *ag) {

  // Add to access group map
  auto res = m_access_group_map.insert(
    make_pair(m_arena.dup(ag->get_name()), ag));
  if (!res.second)
    HT_THROWF(Error::BAD_SCHEMA,
              "Attempt to add access group '%s' which already exists",
              ag->get_name().c_str());

  set<string> column_families;
  for (auto cf : ag->columns())
    column_families.insert(cf->get_name());

  size_t column_count {};
  for (auto ag : m_access_groups) {
    column_count += ag->columns().size();
    for (auto cf : ag->columns()) {
      if (column_families.count(cf->get_name()))
        HT_THROWF(Error::BAD_SCHEMA,
                  "Column family '%s' already defined",
                  cf->get_name().c_str());
    }
  }

  if (column_count + ag->columns().size() > MAX_COLUMN_ID)
    HT_THROWF(Error::TOO_MANY_COLUMNS,
              "Attempt to add > %d column families to table",
              MAX_COLUMN_ID);    

  // Add to access group vector
  m_access_groups.push_back(ag);

  // Merge table defaults into added access group
  merge_table_defaults(ag);

  // Merge table defaults into each added column
  for (auto cf : ag->columns())
    merge_table_defaults(cf);

}

ColumnFamilySpec *Schema::remove_column_family(const string &name) {
  auto iter = m_column_family_map.find(name.c_str());
  if (iter != m_column_family_map.end()) {
    ColumnFamilySpec *cf = iter->second;
    auto ag_map_iter = m_access_group_map.find(cf->get_access_group().c_str());
    HT_ASSERT(ag_map_iter != m_access_group_map.end());
    ag_map_iter->second->remove_column(name);
    if (cf->get_id()) {
      m_column_family_id_map.erase(cf->get_id());
      m_counter_mask[cf->get_id()] = false;
    }
    m_column_family_map.erase(iter);
    return cf;
  }
  return nullptr;
}


AccessGroupSpec *Schema::replace_access_group(AccessGroupSpec *new_ag) {
  AccessGroupSpec *old_ag {};
  auto iter = m_access_group_map.find(new_ag->get_name().c_str());
  if (iter != m_access_group_map.end()) {
    old_ag = iter->second;
    m_access_group_map.erase(iter);
    AccessGroupSpecs new_ags(m_access_groups.size());
    auto it = copy_if(m_access_groups.begin(), m_access_groups.end(),
                      new_ags.begin(),
                      [old_ag](AccessGroupSpec *ag){return ag != old_ag;});
    new_ags.resize(distance(new_ags.begin(), it));
    m_access_groups.swap(new_ags);
  }
  add_access_group(new_ag);
  return old_ag;
}

bool Schema::column_family_exists(int32_t id, bool get_deleted) const
{
  auto cf_iter = m_column_family_id_map.find(id);
  if (cf_iter != m_column_family_id_map.end()) {
    if (get_deleted || !cf_iter->second->get_deleted())
      return true;
  }
  return false;
}

bool Schema::access_group_exists(const string &name) const
{
  auto ag_iter = m_access_group_map.find(name.c_str());
  return (ag_iter != m_access_group_map.end());
}

void Schema::rename_column_family(const string &old_name, const string &new_name) {
  ColumnFamilySpec *cf;

  // update key and ColumnFamily in m_column_family_map
  auto cf_map_it = m_column_family_map.find(old_name.c_str());
  if (cf_map_it == m_column_family_map.end())
    HT_THROWF(Error::INVALID_OPERATION,
              "Source column '%s' of rename does not exist", old_name.c_str());

  if (old_name != new_name) {
    cf = cf_map_it->second;
    cf->set_name(new_name);
    cf->set_generation(0);
    auto res = m_column_family_map.insert(
      make_pair(m_arena.dup(cf->get_name()), cf));
    if (!res.second)
    HT_THROWF(Error::INVALID_OPERATION,
              "Destination column '%s' of rename already exists",
              cf->get_name().c_str());
    m_column_family_map.erase(cf_map_it);
  }
}

void Schema::drop_column_family(const string &name) {

  auto cf_map_it = m_column_family_map.find(name.c_str());
  if (cf_map_it == m_column_family_map.end())
    HT_THROWF(Error::INVALID_OPERATION,
              "Column family to drop (%s) does not exist",
              name.c_str());

  ColumnFamilySpec *cf = cf_map_it->second;

  auto ag_it = m_access_group_map.find(cf->get_access_group().c_str());
  if (ag_it == m_access_group_map.end())
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Invalid access group '%s' for column family '%s'",
              cf->get_access_group().c_str(), cf->get_name().c_str());

  auto ag_cfs_it = find(ag_it->second->columns().begin(),
                        ag_it->second->columns().end(), cf);
  if (ag_cfs_it == ag_it->second->columns().end())
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Column family '%s' not found in access group '%s'",
              cf->get_name().c_str(), cf->get_access_group().c_str());

  auto cfs_it = find(m_column_families.begin(),
                     m_column_families.end(), cf);
  if (cfs_it == m_column_families.end())
    HT_THROWF(Error::SCHEMA_PARSE_ERROR,
              "Column family '%s' not found in Schema list of columns",
              cf->get_name().c_str());

  ag_it->second->drop_column(name);

  m_column_family_map.erase(cf_map_it);
}

void Schema::validate() {
  size_t column_count {};
  map<string, string> column_to_ag_map;
  m_column_families.clear();
  m_column_family_map.clear();
  m_column_family_id_map.clear();
  m_counter_mask.clear();
  m_counter_mask.resize(256);
  for (auto ag_spec : m_access_groups) {
    column_count += ag_spec->columns().size();
    if (column_count > MAX_COLUMN_ID)
        HT_THROWF(Error::TOO_MANY_COLUMNS,
                  "Attempt to add > %d column families to table",
                  MAX_COLUMN_ID);
    for (auto cf_spec : ag_spec->columns()) {
      if (cf_spec->get_access_group().empty())
        cf_spec->set_access_group(ag_spec->get_name());
      auto iter = column_to_ag_map.find(cf_spec->get_name());
      if (iter != column_to_ag_map.end())
        HT_THROWF(Error::BAD_SCHEMA,
                  "Column '%s' assigned to two access groups (%s & %s)",
                  cf_spec->get_name().c_str(), iter->second.c_str(),
                  ag_spec->get_name().c_str());
      column_to_ag_map.insert(make_pair(cf_spec->get_name(), ag_spec->get_name()));
      m_column_families.push_back(cf_spec);
      m_column_family_map.insert(make_pair(m_arena.dup(cf_spec->get_name()), cf_spec));
      if (cf_spec->get_id()) {
        m_column_family_id_map.insert(make_pair(cf_spec->get_id(), cf_spec));
        if (cf_spec->get_option_counter())
          m_counter_mask[cf_spec->get_id()] = true;
      }
    }
  }
}

void Schema::merge_table_defaults(ColumnFamilySpec *cf_spec) {
  try {
    cf_spec->merge_options(m_cf_defaults);
  }
  catch (Exception &e) {
    HT_THROWF(e.code(), "Merging column '%s' options with table defaults: %s",
              cf_spec->get_name().c_str(), e.what());
  }
}

void Schema::merge_table_defaults(AccessGroupSpec *ag_spec) {
  try {
    ag_spec->merge_options(m_ag_defaults);
  }
  catch (Exception &e) {
    HT_THROWF(e.code(), "Merging access group '%s' options with table defaults: %s",
              ag_spec->get_name().c_str(), e.what());
  }
}
