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

#include <Common/Compat.h>

#include "NameIdMapper.h"

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/ScopeGuard.h>
#include <Common/StringExt.h>

#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>

namespace Hypertable {

using namespace std;
using namespace Hyperspace;


NameIdMapper::NameIdMapper(Hyperspace::SessionPtr &hyperspace, const string &toplevel_dir)
  : m_hyperspace(hyperspace), m_toplevel_dir(toplevel_dir) {

  /*
   * Prefix looks like this:  "/" <toplevel_dir> "namemap" "names"
   * This for loop adds the number of components in <toplevel_dir>
   */
  m_prefix_components = 3;
  for (const char *ptr=toplevel_dir.c_str(); *ptr; ptr++)
    if (*ptr == '/')
      m_prefix_components++;

  m_names_dir = toplevel_dir + "/namemap/names";
  m_ids_dir = toplevel_dir + "/namemap/ids";

  // Create "names" directory
  m_hyperspace->mkdirs(m_names_dir);

  // Create "ids" directory
  std::vector<Attribute> init_attr;
  init_attr.push_back(Attribute("nid", "0", 1));
  m_hyperspace->mkdirs(m_ids_dir, init_attr);
}

bool NameIdMapper::name_to_id(const string &name, string &id, bool *is_namespacep) {
  lock_guard<mutex> lock(m_mutex);
  return do_mapping(name, false, id, is_namespacep);
}

bool NameIdMapper::id_to_name(const string &id, string &name, bool *is_namespacep) {
  lock_guard<mutex> lock(m_mutex);
  return do_mapping(id, true, name, is_namespacep);
}

void NameIdMapper::add_entry(const string &names_parent, const string &names_entry,
                             std::vector<uint64_t> &ids, bool is_namespace) {
  uint64_t id = 0;
  string names_file = m_names_dir + names_parent + "/" + names_entry;

  string parent_ids_file = m_ids_dir;
  for (size_t i=0; i<ids.size(); i++)
    parent_ids_file += String("/") + ids[i];

  if (!m_hyperspace->exists(names_file))
    id = m_hyperspace->attr_incr(parent_ids_file, "nid");
  else {
    bool attr_exists;
    DynamicBuffer dbuf;
    m_hyperspace->attr_get(names_file, "id", attr_exists, dbuf);
    if (attr_exists)
      id = strtoll((const char *)dbuf.base, 0, 0);
    else {
      id = m_hyperspace->attr_incr(parent_ids_file, "nid");
      UInt64Formatter buf(id);
      m_hyperspace->attr_set(names_file, "id", buf.c_str(), buf.size());
    }
    ids.push_back(id);
    return;
  }

  // At this point "id" is properly set
  string ids_file = parent_ids_file + String("/") + id;
  std::vector<Attribute> attrs;
  attrs.push_back(Attribute("name", names_entry.c_str(), names_entry.length()));
  attrs.push_back(Attribute("nid", "0", 1));

  if (m_hyperspace->exists(ids_file)) {
    if (is_namespace) {
      if (!m_hyperspace->attr_exists(ids_file, "nid")) {
        m_hyperspace->attr_set(ids_file, 0, attrs);
      }
    }
  }
  else {
    if (is_namespace) {
      try {
        m_hyperspace->mkdir(ids_file, attrs);
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
        throw;
      }
    }
    else
      m_hyperspace->attr_set(ids_file, OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE|OPEN_FLAG_EXCL,
                             "name", names_entry.c_str(), names_entry.length());
  }

  // At this point the ID file exists, we now need to
  // create the names file/dir and update the "id" attribute

  UInt64Formatter buf(id);

  if (is_namespace) {
    std::vector<Attribute> init_attr;
    init_attr.push_back(Attribute("id", buf.c_str(), buf.size()));
    m_hyperspace->mkdir(names_file, init_attr);
  }
  else {
    // Set the "id" attribute of the names file
    m_hyperspace->attr_set(names_file, OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE|OPEN_FLAG_EXCL,
                           "id", buf.c_str(), buf.size());
  }
  ids.push_back(id);
}

void NameIdMapper::add_mapping(const string &name, string &id, int flags, bool ignore_exists) {
  lock_guard<mutex> lock(m_mutex);
  typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
  boost::char_separator<char> sep("/");
  std::vector<String> name_components;
  std::vector<uint64_t> id_components;
  std::vector<DirEntryAttr> listing;
  DynamicBuffer value_buf;

  tokenizer tokens(name, sep);
  for (tokenizer::iterator tok_iter = tokens.begin();
       tok_iter != tokens.end(); ++tok_iter)
    name_components.push_back(*tok_iter);

  HT_ASSERT(!name_components.empty());

  string names_parent = "";
  string names_child = "";

  listing.reserve(name_components.size());
  for (size_t i=0; i<name_components.size()-1; i++) {

    names_child += String("/") + name_components[i];

    try {
      string names_file = m_names_dir + names_child;
      m_hyperspace->attr_get(names_file, "id", value_buf);
      if (flags & CREATE_INTERMEDIATE) {
        m_hyperspace->readpath_attr(names_file, "id", listing);
        if (!listing.back().is_dir)
          HT_THROW(Error::NAME_ALREADY_IN_USE, names_child);
      }
      id_components.push_back( strtoll((const char *)value_buf.base, 0, 0) );
    }
    catch (Exception &e) {

      if (e.code() != Error::HYPERSPACE_BAD_PATHNAME &&
          e.code() != Error::HYPERSPACE_FILE_NOT_FOUND)
        throw;

      if (!(flags & CREATE_INTERMEDIATE))
        HT_THROW(Error::NAMESPACE_DOES_NOT_EXIST, names_child);

      add_entry(names_parent, name_components[i], id_components, true);
    }

    names_parent = names_child;
  }

  try {
    add_entry(names_parent, name_components.back(),
        id_components, (bool)(flags & IS_NAMESPACE));
  }
  catch (Exception &e) {
    if (!ignore_exists && e.code() == Error::HYPERSPACE_FILE_EXISTS)
      HT_THROW(Error::NAMESPACE_EXISTS, (String)" namespace=" + name);
    throw;
  }

  id = (name[0] == '/') ? "/" : "";
  id = id + id_components[0];

  for (size_t i=1; i<id_components.size(); i++)
    id += String("/") + id_components[i];
}

void NameIdMapper::rename(const string &curr_name, const string &next_name) {
  lock_guard<mutex> lock(m_mutex);
  string id;
  int oflags = OPEN_FLAG_WRITE|OPEN_FLAG_EXCL|OPEN_FLAG_READ;
  string old_name = curr_name;
  string new_name = next_name;
  string new_name_last_comp;
  size_t new_name_last_slash_pos;
  string id_last_component;
  size_t id_last_component_pos;

  boost::trim_if(old_name, boost::is_any_of("/ "));
  boost::trim_if(new_name, boost::is_any_of("/ "));

  new_name_last_slash_pos = new_name.find_last_of('/');
  if (new_name_last_slash_pos != String::npos)
    new_name_last_comp = new_name.substr(new_name_last_slash_pos+1);
  else
    new_name_last_comp = new_name;

  if (do_mapping(old_name, false, id, 0)) {
    // Set the name attribute of the id file to be the last path component of new_name
    string id_file = m_ids_dir + "/" + id;
    m_hyperspace->attr_set(id_file, oflags, "name", new_name_last_comp.c_str(),
                           new_name_last_comp.length());

    // Create the name file and set its id attribute
    id_last_component_pos = id.find_last_of('/');
    if (id_last_component_pos != String::npos)
      id_last_component = id.substr(id_last_component_pos+1);
    else
      id_last_component = id;

    m_hyperspace->attr_set(m_names_dir + "/" + new_name, oflags|OPEN_FLAG_CREATE, "id", id_last_component.c_str(),
                           id_last_component.length());

    // Delete the existing name file
    m_hyperspace->unlink(m_names_dir + "/" + old_name);
  }

}

void NameIdMapper::drop_mapping(const string &name) {
  lock_guard<mutex> lock(m_mutex);
  string id;
  string table_name = name;

  boost::trim_if(table_name, boost::is_any_of("/ "));

  if (do_mapping(name, false, id, 0)) {
    try {
      m_hyperspace->unlink(m_ids_dir + "/" + id);
    }
    catch (Exception &e) {
      if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND &&
          e.code() != Error::HYPERSPACE_BAD_PATHNAME)
        throw;
    }    
  }

  try {
    m_hyperspace->unlink(m_names_dir + "/" + table_name);
  }
  catch (Exception &e) {
    if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND &&
        e.code() != Error::HYPERSPACE_BAD_PATHNAME)
      throw;
  }
}

bool NameIdMapper::exists_mapping(const string &name, bool *is_namespace) {
  string id;
  string namespace_name = name;
  boost::trim_if(namespace_name, boost::is_any_of("/ "));
  return do_mapping(namespace_name, false, id, is_namespace);
}

bool NameIdMapper::do_mapping(const string &input, bool id_in, string &output,
                              bool *is_namespacep) {
  vector <DirEntryAttr> listing;
  int num_path_components = 0;
  string hyperspace_file;
  string attr;
  output = (String)"";

  if (id_in) {
    hyperspace_file = m_ids_dir;
    attr = (String)"name";
  }
  else {
    hyperspace_file = m_names_dir;
    attr = (String)"id";
  }

  // deal with leading '/' in input
  if (input.find('/') != 0)
    hyperspace_file += "/";

  hyperspace_file += input;

  // count number of components in path (ignore  trailing '/')
  num_path_components = 1;
  for(size_t ii=0; ii< hyperspace_file.size()-1; ++ii) {
    if (hyperspace_file[ii] == '/')
      ++num_path_components;
  }

  try {
    m_hyperspace->readpath_attr(hyperspace_file, attr, listing);
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME) {
      HT_DEBUG_OUT << "Can't map " << input << HT_END;
      return false;
    }
    throw;
  }

  if (listing.size() != (size_t) num_path_components || listing.size() == 0)
    return false;

  struct LtDirEntryAttr ascending;
  sort(listing.begin(), listing.end(), ascending);

  for (size_t ii=m_prefix_components; ii<listing.size(); ++ii) {
    string attr_val((const char*)listing[ii].attr.base);
    output += attr_val + "/";
  }

  // strip trailing slash
  if (!output.empty())
    output.resize(output.size()-1);

  bool is_dir = listing[listing.size()-1].is_dir;

  if (is_namespacep)
    *is_namespacep = is_dir;

  return true;

}

void NameIdMapper::id_to_sublisting(const string &id, bool include_sub_entries, vector<NamespaceListing> &listing) {
  vector <DirEntryAttr> dir_listing;
  string hyperspace_dir;
  string attr;

  hyperspace_dir = m_ids_dir;
  attr = (String)"name";

  hyperspace_dir += (String)"/" + id;

  m_hyperspace->readdir_attr(hyperspace_dir, attr, include_sub_entries, dir_listing);

  get_namespace_listing(dir_listing, listing);
}

void NameIdMapper::get_namespace_listing(const std::vector<DirEntryAttr> &dir_listing, std::vector<NamespaceListing> &listing) {
  NamespaceListing entry;
  listing.clear();
  listing.reserve(dir_listing.size());
  for (const auto &dir_entry : dir_listing) {
    if (dir_entry.has_attr) {
      entry.name = (String)((const char*)dir_entry.attr.base);
      entry.is_namespace = dir_entry.is_dir;
      entry.id = dir_entry.name;
      listing.push_back(entry);
      if (!dir_entry.sub_entries.empty())
        get_namespace_listing(dir_entry.sub_entries, listing.back().sub_entries);
    }
  }

  struct LtNamespaceListingName ascending;
  sort(listing.begin(), listing.end(), ascending);
}
} // namespace Hypertable

