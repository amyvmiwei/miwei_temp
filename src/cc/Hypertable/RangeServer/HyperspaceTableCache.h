/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
 * Declarations for HyperspaceTableCache.
 * This file contains the type declarations for HyperspaceTableCache, a class used to
 * load table schemas from Hyperspace into memory and provide fast lookup.
 */

#ifndef Hypertable_HyperspaceTableCache_h
#define Hypertable_HyperspaceTableCache_h

#include <map>

#include <boost/intrusive_ptr.hpp>

#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/Schema.h"

#include "Hyperspace/Session.h"

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Provides efficient lookup of Hyperspace table data.
  /// This class efficiently reads schema and <i>maintenance_disabled</i> attribute
  /// for all tables in Hyperspace into memory and provides an API for fast
  /// lookup.
  class HyperspaceTableCache : public ReferenceCount {
  public:

    /// Cache entry for Hyperspace table data
    class Entry {
    public:
      /// Smart pointer to Schema object
      SchemaPtr schema;
      /// Flag indicating if "maintenance_disabled" attribute is set
      bool maintenance_disabled {};
    };

    /// Constructor.
    /// This constructor reads table information (schemas and
    /// maintenance_disabled attribute) from Hyperspace and caches the
    /// information in an in-memory map (#m_map) with the key being the table
    /// ID.
    /// @param hyperspace &Hyperspace session
    /// @param toplevel_dir Toplevel hyperspace directory
    HyperspaceTableCache(Hyperspace::SessionPtr &hyperspace,
                     const String &toplevel_dir);

    /// Returns Hyperspace cache entry for a table.
    /// @param table_id %Table identifier string
    /// @param schema Output parameter to hold Schema object
    /// @return <i>true</i> if cache entry found for <code>table_id</code>,
    /// <i>false</i> otherwise.
    bool get(const String &table_id, Entry &entry);

  private:

    /// Recursively populates map with table schemas.
    /// @param parent Parent ID pathname
    /// @param listing Vector of directory entries within <code>parent</code>
    void map_table_schemas(const String &parent,
                           const std::vector<Hyperspace::DirEntryAttr> &listing);

    /// Recursively updates maintenance_disabled field in map entries.
    /// @param parent Parent ID pathname
    /// @param listing Vector of directory entries within <code>parent</code>
    void map_maintenance_disabled(const String &parent,
                                  const std::vector<Hyperspace::DirEntryAttr> &listing);

    /// %Table ID to Entry map type.
    typedef std::map<String, Entry> TableEntryMap;

    /// %Table ID to Entry map
    TableEntryMap m_map;
  };

  /// Smart pointer to HyperspaceTableCache
  typedef boost::intrusive_ptr<HyperspaceTableCache> HyperspaceTableCachePtr;

  /// @}
}

#endif // Hypertable_HyperspaceTableCache_h
