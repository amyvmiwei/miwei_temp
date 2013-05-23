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
 * Declarations for TableSchemaCache.
 * This file contains the type declarations for TableSchemaCache, a class used to
 * load table schemas from Hyperspace into memory and provide fast lookup.
 */

#ifndef HYPERTABLE_TABLESCHEMACACHE_H
#define HYPERTABLE_TABLESCHEMACACHE_H

#include <map>

#include <boost/intrusive_ptr.hpp>

#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/Schema.h"

#include "Hyperspace/Session.h"

namespace Hypertable {

  /** @addtogroup RangeServer
   * @{
   */

  /** Provides efficient lookup of Schema objects.
   * This class efficiently reads all of the schemas in Hyperspace into
   * memory and provides an API for fast lookups.
   */
  class TableSchemaCache : public ReferenceCount {
  public:

    /** Constructor.
     * This constructor efficiently reads all of the schemas from the
     * <i>tables</i> directory in Hyperspace using a single recursive Hyperspace
     * call (Hyperspace::Session::readdir_attr).  For each schema read, a Schema
     * object is constructed and added to an in-memory map with the key being
     * the table identifier string.
     * @param hyperspace Hyperspace session object
     * @param toplevel_dir Toplevel hyperspace directory
     */
    TableSchemaCache(Hyperspace::SessionPtr &hyperspace,
                     const String &toplevel_dir);

    /** Returns the schema object for a table.
     * @param table_id Table identifier string
     * @param schema Output parameter to hold Schema object
     * @return <i>true</i> if schema found for <code>table_id</code>,
     * <i>false</i> otherwise.
     */
    bool get(const String &table_id, SchemaPtr &schema);

  private:

    /** Recursively populates map from vector of directory entries.
     * @param parent Parent ID pathname
     * @param listing Vector of directory entries within <code>parent</code>
     */
    void map_table_schemas(const String &parent,
                           const std::vector<Hyperspace::DirEntryAttr> &listing);

    /// table_id-to-Schema map type
    typedef std::map<String, SchemaPtr> TableSchemaMap;

    /// table_id-to-Schema map
    TableSchemaMap m_map;
  };

  /// Smart pointer to TableSchemaCache
  typedef boost::intrusive_ptr<TableSchemaCache> TableSchemaCachePtr;

  /* @} */
}

#endif // HYPERTABLE_TABLESCHEMACACHE_H
