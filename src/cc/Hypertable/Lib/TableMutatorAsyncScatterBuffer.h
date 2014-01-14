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

/// @file
/// Declarations for TableMutatorAsyncScatterBuffer.
/// This file contains declarations for TableMutatorAsyncScatterBuffer, a class
/// for sending updates to to a set of range servers in parallel.

#ifndef HYPERTABLE_TABLEMUTATORASYNCSCATTERBUFFER_H
#define HYPERTABLE_TABLEMUTATORASYNCSCATTERBUFFER_H

#include <Hypertable/Lib/Cell.h>
#include <Hypertable/Lib/Cells.h>
#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/RangeLocator.h>
#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/TableMutatorAsyncSendBuffer.h>
#include <Hypertable/Lib/TableMutatorAsyncCompletionCounter.h>

#include <AsyncComm/CommAddress.h>
#include <AsyncComm/ApplicationQueueInterface.h>
#include <AsyncComm/Event.h>

#include <Common/atomic.h>
#include <Common/ByteString.h>
#include <Common/FlyweightString.h>
#include <Common/ReferenceCount.h>
#include <Common/StringExt.h>
#include <Common/Timer.h>
#include <Common/InetAddr.h>

#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include <vector>

namespace Hypertable {

  class TableMutatorAsync;
  class TableMutatorAsyncScatterBuffer : public ReferenceCount {

  public:
    TableMutatorAsyncScatterBuffer(Comm *comm, ApplicationQueueInterfacePtr &app_queue,
                                   TableMutatorAsync *mutator,
                                   const TableIdentifier *,
                                   SchemaPtr &, RangeLocatorPtr &, bool auto_refresh,
                                   uint32_t timeout_ms,
                                   uint32_t id);
    virtual ~TableMutatorAsyncScatterBuffer();
    void set(const Key &, const void *value, uint32_t value_len, size_t incr_mem);
    void set_delete(const Key &key, size_t incr_mem);
    void set(SerializedKey key, ByteString value, size_t incr_mem);
    bool full() { ScopedLock lock(m_mutex); return m_full; }
    void send(uint32_t flags);
    void wait_for_completion();
    TableMutatorAsyncScatterBuffer *create_redo_buffer(uint32_t id);
    uint64_t get_resend_count() { return m_resends; }
    void
    get_failed_mutations(FailedMutations &failed_mutations) {
      failed_mutations = m_failed_mutations;
    }
    size_t get_failure_count() { return m_failed_mutations.size(); }
    void refresh_schema(const TableIdentifier &table_id, SchemaPtr &schema) {
      m_schema = schema;
      m_table_identifier = table_id;
    }

    uint32_t get_id() const { return m_id; }
    uint32_t get_send_flags() const { return m_send_flags; }
    const CommAddressSet &get_unsynced_rangeservers() { return m_unsynced_rangeservers; }
    /**
     * Returns the amount of memory used by the collected mutations.
     *
     * @return amount of memory used by the collected mutations.
     */
    size_t memory_used() const { return m_memory_used; }
    void set_memory_used(size_t mem) { m_memory_used = mem; }
    void finish();
    void set_retries_to_fail(int error);

  private:
    int set_failed_mutations();
    typedef CommAddressMap<TableMutatorAsyncSendBufferPtr> TableMutatorAsyncSendBufferMap;

    Comm                *m_comm;
    ApplicationQueueInterfacePtr  m_app_queue;
    TableMutatorAsync   *m_mutator;
    SchemaPtr            m_schema;
    RangeLocatorPtr      m_range_locator;
    LocationCachePtr     m_location_cache;
    RangeServerClient    m_range_server;
    TableIdentifierManaged m_table_identifier;
    TableMutatorAsyncSendBufferMap m_buffer_map;
    TableMutatorAsyncCompletionCounter m_completion_counter;
    bool                 m_full;
    uint64_t             m_resends;
    FailedMutations      m_failed_mutations;
    FlyweightString      m_constant_strings;
    boost::mt19937       m_rng;
    bool                 m_auto_refresh;
    uint32_t             m_timeout_ms;
    uint32_t             m_server_flush_limit;
    DynamicBuffer        m_counter_value;
    Timer                m_timer;
    uint32_t             m_id;
    CommAddressSet       m_unsynced_rangeservers;
    size_t               m_memory_used;
    Mutex                m_mutex;
    boost::condition     m_cond;
    bool                 m_outstanding;
    uint32_t             m_send_flags;
    uint32_t             m_wait_time;
    const static uint32_t ms_init_redo_wait_time=1000;
    bool dead;
  };

  typedef intrusive_ptr<TableMutatorAsyncScatterBuffer> TableMutatorAsyncScatterBufferPtr;

} // namespace Hypertable

#endif // HYPERTABLE_TABLEMUTATORASYNCSCATTERBUFFER_H
