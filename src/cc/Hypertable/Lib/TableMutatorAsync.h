/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_TABLEMUTATORASYNC_H
#define HYPERTABLE_TABLEMUTATORASYNC_H

#include <iostream>

#include "AsyncComm/ConnectionManager.h"

#include "Common/Properties.h"
#include "Common/StringExt.h"
#include "Common/Timer.h"

#include "Cells.h"
#include "KeySpec.h"
#include "Table.h"
#include "TableMutatorAsyncScatterBuffer.h"
#include "RangeLocator.h"
#include "RangeServerClient.h"
#include "Schema.h"
#include "Types.h"

namespace Hypertable {

  class TableMutatorAsync;
  typedef intrusive_ptr<TableMutatorAsync> TableMutatorAsyncPtr;

  class IndexMutatorCallback;
  typedef intrusive_ptr<IndexMutatorCallback> IndexMutatorCallbackPtr;

  class TableMutator;

  /**
   * Provides the ability to mutate a table in the form of adding and deleting
   * rows and cells.  Objects of this class are used to collect mutations and
   * periodically flush them to the appropriate range servers.  There is a 1 MB
   * buffer of mutations for each range server.  When one of the buffers fills
   * up all the buffers are flushed to their respective range servers.
   */
  class TableMutatorAsync : public ReferenceCount {

  public:

    /**
     * Constructs the TableMutatorAsync object
     *
     * @param props reference to properties smart pointer
     * @param comm pointer to the Comm layer
     * @param app_queue pointer to the Application Queue
     * @param table pointer to the table object
     * @param range_locator smart pointer to range locator
     * @param timeout_ms maximum time in milliseconds to allow methods
     *        to execute before throwing an exception
     * @param cb callback for this mutator
     * @param flags rangeserver client update command flags
     * @param explicit_block_only if true TableMutatorAsync will not auto_flush or
     *     wait_for_completion unless explicitly told to do so by caller
     */
    TableMutatorAsync(PropertiesPtr &props, Comm *comm,
		      ApplicationQueueInterfacePtr &app_queue, Table *table,
		      RangeLocatorPtr &range_locator, uint32_t timeout_ms, ResultCallback *cb,
		      uint32_t flags = 0, bool explicit_block_only = false);

    TableMutatorAsync(Mutex &mutex, boost::condition &cond, PropertiesPtr &props, Comm *comm,
		      ApplicationQueueInterfacePtr &app_queue, Table *table,
		      RangeLocatorPtr &range_locator, uint32_t timeout_ms, ResultCallback *cb,
		      uint32_t flags = 0, bool explicit_block_only = false,
              TableMutator *mutator = 0);

    /**
     * Destructor for TableMutatorAsync object
     * Make sure buffers are flushed and unsynced rangeservers get synced.
     */
    ~TableMutatorAsync();

    /**
     * Returns the amount of memory used by the collected mutations in the current buffer.
     *
     * @return amount of memory used by the collected mutations.
     */
    virtual uint64_t memory_used() { return m_memory_used; }

    /**
     * There are certain circumstances when mutations get flushed to the wrong
     * range server due to stale range location information.  When the correct
     * location information is discovered, these mutations get resent to the
     * proper range server.  This method returns the number of mutations that
     * were resent.
     *
     * @return number of mutations that were resent
     */
    uint64_t get_resend_count() { return m_resends; }

    /**
     * Inserts a cell into the table.
     *
     * @param key key of the cell being inserted
     * @param value pointer to the value to store in the cell
     * @param value_len length of data pointed to by value
     */
    void set(const KeySpec &key, const void *value, uint32_t value_len);

    /**
     * Convenient helper for null-terminated values
     */
    void set(const KeySpec &key, const char *value) {
      if (value)
        set(key, value, strlen(value));
      else
        set(key, 0, 0);
    }

    /**
     * Convenient helper for String values
     */
    void set(const KeySpec &key, const String &value) {
      set(key, value.data(), value.length());
    }

    /**
     * Deletes an entire row, a column family in a particular row, or a specific
     * cell within a row.
     *
     * @param key key of the row or cell(s) being deleted
     */
    void set_delete(const KeySpec &key);

    /**
     * Insert a bunch of cells into the table (atomically if cells are in
     * the same range/row)
     *
     * @param cells a list of cells
     */
    void set_cells(const Cells &cells) {
      set_cells(cells.begin(), cells.end());
    }

    /**
     * Insert a bunch of cells into the table (atomically if cells are in
     * the same range/row)
     *
     * @param start Iterator pointing to start of cells to be inserted
     * @param end Iterator pointing to end of cells to be inserted
     */
    void set_cells(Cells::const_iterator start, Cells::const_iterator end);

    /**
     * Flushes the current buffer accumulated mutations to their respective range servers.
     * @param sync if false then theres no guarantee that the data is synced disk
     */
    void flush(bool sync=true);

    /**
     * This is where buffers call back into when their outstanding operations are complete
     * @param id id of the buffer
     * @param error error code for finished buffer
     * @param retry true if buffer has retries
     */
    void buffer_finish(uint32_t id, int error, bool retry);
    void cancel();
    bool is_cancelled();
    void get_unsynced_rangeservers(std::vector<CommAddress> &unsynced);
    TableMutatorAsyncScatterBufferPtr get_outstanding_buffer(size_t id);
    bool retry(uint32_t timeout_ms);
    void update_outstanding(TableMutatorAsyncScatterBufferPtr &buffer);
    void get_failed_mutations(FailedMutations &failed_mutations) {
      ScopedLock lock(m_member_mutex);
      failed_mutations = m_failed_mutations;
    }
    bool has_outstanding() {
      ScopedLock lock(m_mutex);
      return !m_outstanding_buffers.empty();
    }
    bool has_outstanding_unlocked() {
      return !m_outstanding_buffers.empty();
    }
    bool needs_flush();

    SchemaPtr schema() { ScopedLock lock(m_mutex); return m_schema; }

  protected:
    void wait_for_completion();

  private:
    /** flush function reserved for use in TableMutator */
    friend class TableMutator;
    void flush_with_tablequeue(TableMutator *mutator, bool sync=true);

    void initialize(PropertiesPtr &props);

    void initialize_indices(PropertiesPtr &props);

    friend class IndexMutatorCallback;
    void update_without_index(const Cell &cell);

    void update_without_index(Key &full_key, const Cell &cell);

    void update_without_index(Key &full_key, const void *value, 
            size_t value_len);

    enum Operation {
      SET = 1,
      SET_CELLS,
      SET_DELETE,
      FLUSH
    };

    /**
     * Calls sync on any unsynced rangeservers and waits for completion
     */
    void do_sync();

    void to_full_key(const void *row, const char *cf, const void *cq,
                     int64_t ts, int64_t rev, uint8_t flag, Key &full_key,
                     Schema::ColumnFamily **pcf = 0);

    void to_full_key(const KeySpec &key, Key &full_key,
                     Schema::ColumnFamily **cf = 0) {
      to_full_key(key.row, key.column_family, key.column_qualifier,
                  key.timestamp, key.revision, key.flag, full_key, cf);
    }

    void to_full_key(const Cell &cell, Key &full_key,
                     Schema::ColumnFamily **cf = 0) {
      to_full_key(cell.row_key, cell.column_family, cell.column_qualifier,
                  cell.timestamp, cell.revision, cell.flag, full_key, cf);
    }

    void update_unsynced_rangeservers(const CommAddressSet &unsynced);

    void handle_send_exceptions(const String& info);

    bool mutated() {
      ScopedLock lock(m_member_mutex);
      return m_mutated;
    }

    bool key_uses_index(Key &key);

    void update_with_index(Key &key, const void *value, uint32_t value_len, 
                           Schema::ColumnFamily *cf);

    typedef std::map<uint32_t, TableMutatorAsyncScatterBufferPtr> ScatterBufferAsyncMap;

    PropertiesPtr        m_props;
    Comm                *m_comm;
    ApplicationQueueInterfacePtr  m_app_queue;
    TablePtr             m_table;
    SchemaPtr            m_schema;  // needs mutex
    RangeLocatorPtr      m_range_locator;
    TableIdentifierManaged m_table_identifier;    // needs mutex
    uint64_t             m_memory_used;  // protected by buffer_mutex
    uint64_t             m_max_memory;
    ScatterBufferAsyncMap  m_outstanding_buffers;  // protected by buffer mutex
    TableMutatorAsyncScatterBufferPtr m_current_buffer; // needs mutex
    uint64_t             m_resends;  // needs mutex
    uint32_t             m_timeout_ms;
    ResultCallback       *m_cb;
    uint32_t             m_flags;
    CommAddressSet       m_unsynced_rangeservers;  // needs mutex

    const static uint32_t ms_max_sync_retries = 5;

    Mutex      m_buffer_mutex;
    Mutex     &m_mutex;
    Mutex      m_member_mutex;
    boost::condition m_buffer_cond;
    boost::condition &m_cond;
    bool       m_explicit_block_only;
    uint32_t   m_next_buffer_id; // needs mutex
    bool       m_cancelled;
    bool       m_mutated;   // needs mutex
    FailedMutations m_failed_mutations;
    TableMutatorAsyncPtr m_index_mutator;
    TableMutatorAsyncPtr m_qualifier_index_mutator;
    IndexMutatorCallbackPtr m_imc;
    bool       m_use_index;
    TableMutator *m_mutator;
  };

} // namespace Hypertable

#endif // HYPERTABLE_TABLEMUTATORASYNC_H
