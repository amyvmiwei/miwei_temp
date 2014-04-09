/* -*- c++ -*-
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

#ifndef HYPERTABLE_TABLE_H
#define HYPERTABLE_TABLE_H

#include "Common/ReferenceCount.h"
#include "Common/Mutex.h"

#include "AsyncComm/ApplicationQueueInterface.h"

#include "ClientObject.h"
#include "NameIdMapper.h"
#include "Schema.h"
#include "RangeLocator.h"
#include "Types.h"
#include "RangeServerProtocol.h"

namespace Hyperspace {
  class Session;
}

namespace Hypertable {

  class ConnectionManager;
  class ResultCallback;
  class TableScannerAsync;
  class TableScanner;
  class TableMutator;
  class TableMutatorAsync;
  class Namespace;

  class Table;
  typedef intrusive_ptr<Table> TablePtr;

  /** Represents an open table.
   */
  class Table : public ClientObject {

  public:

    enum {
      OPEN_FLAG_BYPASS_TABLE_CACHE = 0x01,
      OPEN_FLAG_REFRESH_TABLE_CACHE = 0x02,
      OPEN_FLAG_NO_AUTO_TABLE_REFRESH = 0x04,
      SCANNER_FLAG_IGNORE_INDEX = 0x01
    };

    enum {
      MUTATOR_FLAG_NO_LOG_SYNC = RangeServerProtocol::UPDATE_FLAG_NO_LOG_SYNC
    };

    Table(PropertiesPtr &, ConnectionManagerPtr &, Hyperspace::SessionPtr &,
          NameIdMapperPtr &namemap, const String &name, int32_t flags=0);
    Table(PropertiesPtr &, RangeLocatorPtr &, ConnectionManagerPtr &,
          Hyperspace::SessionPtr &, ApplicationQueueInterfacePtr &, NameIdMapperPtr &,
          const String &name, int32_t flags, uint32_t default_timeout_ms);
    virtual ~Table();

    /**
     * Creates a mutator on this table
     *
     * @param timeout_ms maximum time in milliseconds to allow
     *        mutator methods to execute before throwing an exception
     * @param flags mutator flags
     * @param flush_interval_ms time interval in milliseconds to flush
     *        data. 0 disables it.
     * @return newly constructed mutator object
     */
    TableMutator *create_mutator(uint32_t timeout_ms = 0,
                                 uint32_t flags = 0,
                                 uint32_t flush_interval_ms = 0);
    /**
     * Creates an asynchronous mutator on this table
     *
     * @param cb Pointer to result callback
     * @param timeout_ms maximum time in milliseconds to allow
     *        mutator methods to execute before throwing an exception
     * @param flags mutator flags
     * @return newly constructed mutator object
     */
    TableMutatorAsync *create_mutator_async(ResultCallback *cb,
                                            uint32_t timeout_ms = 0,
                                            uint32_t flags = 0);
    /**
     * Creates a synchronous scanner on this table
     *
     * @param scan_spec scan specification
     * @param timeout_ms maximum time in milliseconds to allow
     *        scanner methods to execute before throwing an exception
     * @param flags scanner flags
     * @return pointer to scanner object
     */
    TableScanner *create_scanner(const ScanSpec &scan_spec,
                                 uint32_t timeout_ms = 0, int32_t flags = 0);


    /**
     * Creates an asynchronous scanner on this table
     *
     * @param cb Callback to be notified when scan results arrive
     * @param scan_spec scan specification
     * @param timeout_ms maximum time in milliseconds to allow
     *        scanner methods to execute before throwing an exception
     * @param flags Scanner flags
     * @return pointer to scanner object
     */
    TableScannerAsync *create_scanner_async(ResultCallback *cb,
                                            const ScanSpec &scan_spec,
                                            uint32_t timeout_ms = 0,
                                            int32_t flags = 0);

    void get_identifier(TableIdentifier *table_id_p) {
      ScopedLock lock(m_mutex);
      refresh_if_required();
      *table_id_p = m_table;
    }

    String get_name() {
      ScopedLock lock(m_mutex);
      return m_name;
    }

    SchemaPtr schema() {
      ScopedLock lock(m_mutex);
      refresh_if_required();
      return m_schema;
    }

    /**
     * Refresh schema etc.
     */
    void refresh();

    /**
     * Get a copy of table identifier and schema atomically
     *
     * @param table_identifier reference of the table identifier copy
     * @param schema reference of the schema copy
     */
    void get(TableIdentifierManaged &table_identifier, SchemaPtr &schema);

    /**
     * Make a copy of table identifier and schema atomically also
     */
    void refresh(TableIdentifierManaged &table_identifier, SchemaPtr &schema);

    bool need_refresh() {
      ScopedLock lock(m_mutex);
      return m_stale;
    }

    void invalidate() {
      ScopedLock lock(m_mutex);
      m_stale = true;
    }

    bool auto_refresh() {
      return (m_flags & OPEN_FLAG_NO_AUTO_TABLE_REFRESH) == 0;
    }

    int32_t get_flags() { return m_flags; }

    /** returns true if this table requires a index table */
    bool needs_index_table() {
      ScopedLock lock(m_mutex);
      foreach_ht (Schema::ColumnFamily *cf, m_schema->get_column_families()) {
        if (cf->deleted)
          continue;
        if (cf->has_index)
          return true;
      }
      return false;
    }

    /** returns true if this table requires a qualifier index table */
    bool needs_qualifier_index_table() {
      ScopedLock lock(m_mutex);
      foreach_ht (Schema::ColumnFamily *cf, m_schema->get_column_families()) {
        if (cf->deleted)
          continue;
        if (cf->has_qualifier_index)
          return true;
      }
      return false;
    }

    /** returns true if this table has an index */
    bool has_index_table() {
      ScopedLock lock(m_mutex);
      return (m_index_table!=0);
    }

    /** returns true if this table has a qualifier index */
    bool has_qualifier_index_table() {
      ScopedLock lock(m_mutex);
      return (m_qualifier_index_table != 0);
    }

    /** sets the index table. The index table is created by the Namespace,
     * because it's the only object with access to the TableCache  */
    void set_index_table(TablePtr idx) {
      ScopedLock lock(m_mutex);
      HT_ASSERT(idx != 0 ? m_index_table == 0 : 1);
      m_index_table = idx;
    }

    /** sets the qualifier index table */
    void set_qualifier_index_table(TablePtr idx) {
      ScopedLock lock(m_mutex);
      HT_ASSERT(idx != 0 ? m_qualifier_index_table == 0 : 1);
      m_qualifier_index_table = idx;
    }

    TablePtr get_index_table() {
      return m_index_table;
    }

    TablePtr get_qualifier_index_table() {
      return m_qualifier_index_table;
    }

    void set_namespace(Namespace *ns) {
      m_namespace = ns;
    }

    Namespace *get_namespace() {
      return m_namespace;
    }

    RangeLocatorPtr get_range_locator() { return m_range_locator; }

  private:
    void initialize();
    void refresh_if_required();

    Mutex                  m_mutex;
    PropertiesPtr          m_props;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager;
    Hyperspace::SessionPtr m_hyperspace;
    SchemaPtr              m_schema;
    RangeLocatorPtr        m_range_locator;
    ApplicationQueueInterfacePtr m_app_queue;
    NameIdMapperPtr        m_namemap;
    String                 m_name;
    TableIdentifierManaged m_table;
    int32_t                m_flags;
    int                    m_timeout_ms;
    bool                   m_stale;
    String                 m_toplevel_dir;
    size_t                 m_scanner_queue_size;
    TablePtr               m_index_table;
    TablePtr               m_qualifier_index_table;
    Namespace             *m_namespace;
  };

} // namespace Hypertable

#endif // HYPERTABLE_TABLE_H
