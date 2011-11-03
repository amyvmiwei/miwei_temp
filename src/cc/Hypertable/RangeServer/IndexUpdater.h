/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

#ifndef HYPERTABLE_INDEXUPDATER_H
#define HYPERTABLE_INDEXUPDATER_H

#include "Common/Compat.h"
#include <map>

#include "Common/ReferenceCount.h"
#include "Common/Mutex.h"
#include "Common/String.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/TableMutatorAsync.h"

#include "Global.h"


namespace Hypertable {

  class ResultCallback;

  /**
   * The IndexUpdater purges keys from an index table. This object is 
   * created once per scan.
   */
  class IndexUpdater : public ReferenceCount {
    friend class IndexUpdaterFactory;

    // constructor; objects are created by IndexUpdaterFactory
    IndexUpdater(SchemaPtr &primary_schema, TablePtr index_table, 
                 TablePtr qualifier_index_table);

  public:
    ~IndexUpdater() {
      if (m_index_mutator)
        delete m_index_mutator;
      if (m_qualifier_index_mutator)
        delete m_qualifier_index_mutator;
      delete m_cb;
    }

    // purges a key from the indices
    void purge(const Key &key, const ByteString &value);

  private:
    TableMutatorAsync *m_index_mutator;
    TableMutatorAsync *m_qualifier_index_mutator;
    ResultCallback    *m_cb;
    bool               m_index_map[256];
    bool               m_qualifier_index_map[256];
    String             m_cf_namemap[256];
    unsigned           m_highest_column_id;
  };

  /**
   * The IndexUpdaterFactory creates new IndexUpdater objects
   */
  class IndexUpdaterFactory {
  public:
    // factory function
    static IndexUpdater *create(const String &table_id, SchemaPtr &schema,
            bool has_index, bool has_qualifier_index);

    // cleanup function; called before leaving main()
    static void close();

  private:
    // loads a table
    static Table *load_table(const String &table_name);

    typedef std::map<String, TablePtr> TableMap;

    static Mutex ms_mutex;
    static NameIdMapperPtr ms_namemap;
    static TableMap ms_qualifier_index_cache;
    static TableMap ms_index_cache;
  };

  typedef boost::intrusive_ptr<IndexUpdater> IndexUpdaterPtr;
} // namespace Hypertable

#endif // HYPERTABLE_INDEXUPDATER_H
