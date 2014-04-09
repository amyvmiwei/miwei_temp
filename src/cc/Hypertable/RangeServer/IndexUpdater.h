/* -*- c++ -*-
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
/// Declarations for IndexUpdater.
/// This file contains type declarations for IndexUpdater, a class for keeping
/// index tables up-to-date.

#ifndef Hypertable_RangeServer_IndexUpdater_h
#define Hypertable_RangeServer_IndexUpdater_h

#include <Hypertable/RangeServer/Global.h>

#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/Table.h>
#include <Hypertable/Lib/TableMutatorAsync.h>

#include <Common/Mutex.h>
#include <Common/String.h>

#include <map>
#include <memory>

namespace Hypertable {

  class ResultCallback;

  /// @addtogroup RangeServer
  /// @{

  /// Helper class for updating index tables.
  class IndexUpdater {
    friend class IndexUpdaterFactory;

  public:

    /// constructor; objects are created by IndexUpdaterFactory
    IndexUpdater(SchemaPtr &primary_schema, TablePtr index_table, 
                 TablePtr qualifier_index_table);

    /// Destructor.
    ~IndexUpdater() {
      if (m_index_mutator)
        delete m_index_mutator;
      if (m_qualifier_index_mutator)
        delete m_qualifier_index_mutator;
      delete m_cb;
    }

    /// Purges a key from index tables.
    void purge(const Key &key, const ByteString &value);

    /// Adds a key to index tables.
    void add(const Key &key, const ByteString &value);

  private:
    TableMutatorAsync *m_index_mutator;
    TableMutatorAsync *m_qualifier_index_mutator;
    ResultCallback    *m_cb;
    bool               m_index_map[256];
    bool               m_qualifier_index_map[256];
    String             m_cf_namemap[256];
    unsigned           m_highest_column_id;
  };

  /// Smart pointer to IndexUpdater.
  typedef std::shared_ptr<IndexUpdater> IndexUpdaterPtr;

  /// Factory class for creating IndexUpdater objects.
  class IndexUpdaterFactory {
  public:
    // Factory function.
    static IndexUpdaterPtr create(const String &table_id, SchemaPtr &schema,
            bool has_index, bool has_qualifier_index);

    // Cleanup function; called before leaving main()
    static void close();

    /// Clears both value and qualifier caches
    static void clear_cache();

  private:
    // Loads a table.
    static Table *load_table(const String &table_name);

    typedef std::map<String, TablePtr> TableMap;

    static Mutex ms_mutex;
    static NameIdMapperPtr ms_namemap;
    static TableMap ms_qualifier_index_cache;
    static TableMap ms_index_cache;
  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_RangeServer_IndexUpdater_h
