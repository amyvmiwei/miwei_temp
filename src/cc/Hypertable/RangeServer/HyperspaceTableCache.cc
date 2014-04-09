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

/** @file
 * Definitions for HyperspaceTableCache.
 * This file contains method definitions for HyperspaceTableCache, a class used to
 * load table schemas from Hyperspace into memory and provide fast lookup.
 */

#include "Common/Compat.h"

#include "Global.h"
#include "HyperspaceTableCache.h"

#include <vector>

using namespace Hypertable;

HyperspaceTableCache::HyperspaceTableCache(Hyperspace::SessionPtr &hyperspace,
                                   const String &toplevel_dir) {

  try {
    std::vector<Hyperspace::DirEntryAttr> listing;
    hyperspace->readdir_attr(toplevel_dir + "/tables", "schema", true, listing);
    map_table_schemas("", listing);
    listing.clear();
    hyperspace->readdir_attr(toplevel_dir + "/tables", "maintenance_disabled", true, listing);
    map_maintenance_disabled("", listing);
  }
  catch (Exception &e) {
    HT_FATAL_OUT << "Problem loading table schemas " << e << HT_END;
  }  
}

bool HyperspaceTableCache::get(const String &table_id, Entry &entry) {
  auto iter =  m_map.find(table_id);
  if (iter == m_map.end())
    return false;
  entry = iter->second;
  return true;
}


void HyperspaceTableCache::map_table_schemas(const String &parent,
                                         const std::vector<Hyperspace::DirEntryAttr> &listing) {
  String prefix = !parent.empty() ? parent + "/" : ""; // avoid leading slash
  for (auto &e : listing) {
    String name = prefix + e.name;
    if (e.has_attr) {
      Entry entry;
      entry.schema = Schema::new_instance((char*)e.attr.base, e.attr.size);
      m_map.insert(TableEntryMap::value_type(name, entry));
    }
    map_table_schemas(name, e.sub_entries);
  }
}

void
HyperspaceTableCache::map_maintenance_disabled(const String &parent,
                       const std::vector<Hyperspace::DirEntryAttr> &listing) {
  String prefix = !parent.empty() ? parent + "/" : ""; // avoid leading slash
  for (auto &e : listing) {
    String name = prefix + e.name;
    auto iter =  m_map.find(name);
    if (e.has_attr && iter != m_map.end())
      iter->second.maintenance_disabled = true;
    map_maintenance_disabled(name, e.sub_entries);
  }
}

