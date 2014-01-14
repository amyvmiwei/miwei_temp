/* -*- c++ -*-
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

/// @file
/// Definitions for TableMutatorAsyncScatterBuffer.
/// This file contains definitions for TableMutatorAsyncScatterBuffer, a class
/// for sending updates to to a set of range servers in parallel.

#include <Common/Compat.h>

#include "TableMutatorAsyncScatterBuffer.h"

#include <Common/Config.h>
#include <Common/Timer.h>

#include <Hypertable/Lib/ClusterId.h>
#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/KeySpec.h>
#include <Hypertable/Lib/RangeServerClient.h>
#include <Hypertable/Lib/Table.h>
#include <Hypertable/Lib/TableMutatorAsyncDispatchHandler.h>
#include <Hypertable/Lib/TableMutatorAsyncHandler.h>

#include <poll.h>

using namespace Hypertable;

TableMutatorAsyncScatterBuffer::TableMutatorAsyncScatterBuffer(Comm *comm,
    ApplicationQueueInterfacePtr &app_queue, TableMutatorAsync *mutator,
    const TableIdentifier *table_identifier, SchemaPtr &schema,
    RangeLocatorPtr &range_locator, bool auto_refresh, uint32_t timeout_ms, uint32_t id)
  : m_comm(comm), m_app_queue(app_queue), m_mutator(mutator), m_schema(schema),
    m_range_locator(range_locator), m_range_server(comm, timeout_ms),
    m_table_identifier(*table_identifier),
    m_full(false), m_resends(0), m_rng(1), m_auto_refresh(auto_refresh), m_timeout_ms(timeout_ms),
    m_counter_value(9), m_timer(timeout_ms), m_id(id), m_memory_used(0), m_outstanding(false),
    m_send_flags(0), m_wait_time(ms_init_redo_wait_time), dead(false) {

  HT_ASSERT(Config::properties);

  m_location_cache = m_range_locator->location_cache();

  m_server_flush_limit = Config::properties->get_i32(
      "Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer");
}

TableMutatorAsyncScatterBuffer::~TableMutatorAsyncScatterBuffer() {
  dead = true;
}


void
TableMutatorAsyncScatterBuffer::set(const Key &key, const void *value, uint32_t value_len,
    size_t incr_mem) {

  RangeLocationInfo range_info;
  TableMutatorAsyncSendBufferMap::const_iterator iter;
  bool counter_reset = false;

  if (!m_location_cache->lookup(m_table_identifier.id, key.row, &range_info)) {
    Timer timer(m_timeout_ms, true);
    m_range_locator->find_loop(&m_table_identifier, key.row, &range_info,
                               timer, false);
  }

  {
    ScopedLock lock(m_mutex);

    // counter? make sure that a valid integer was specified and re-encode
    // it as a 64bit value
    if (key.column_family_code
	&& m_schema->get_column_family(key.column_family_code)->counter) {
      const char *ascii_value = (const char *)value;
      char *endptr;
      m_counter_value.clear();
      m_counter_value.ensure(value_len+1);
      if (value_len > 0 && (*ascii_value == '=' || *ascii_value == '+')) {
	counter_reset = (*ascii_value == '=');
	m_counter_value.add_unchecked(ascii_value+1, value_len-1);
      }
      else
	m_counter_value.add_unchecked(value, value_len);
      m_counter_value.add_unchecked((const void *)"\0",1);
      int64_t val = strtoll((const char *)m_counter_value.base, &endptr, 0);
      if (*endptr)
	HT_THROWF(Error::BAD_KEY, "Expected integer value, got %s, row=%s",
		  (char*)m_counter_value.base, key.row);
      m_counter_value.clear();
      Serialization::encode_i64(&m_counter_value.ptr, val);
    }

    iter = m_buffer_map.find(range_info.addr);

    if (iter == m_buffer_map.end()) {
      // this can be optimized by using the insert() method
      m_buffer_map[range_info.addr] = new TableMutatorAsyncSendBuffer(&m_table_identifier,
								      &m_completion_counter, m_range_locator.get());
      iter = m_buffer_map.find(range_info.addr);
      (*iter).second->addr = range_info.addr;
    }

    (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
    create_key_and_append((*iter).second->accum, key.flag, key.row,
			  key.column_family_code, key.column_qualifier, key.timestamp);

    // now append the counter
    if (key.column_family_code
	&& m_schema->get_column_family(key.column_family_code)->counter) {
      if (counter_reset) {
	*m_counter_value.ptr++ = '=';
	append_as_byte_string((*iter).second->accum, m_counter_value.base, 9);
      }
      else
	append_as_byte_string((*iter).second->accum, m_counter_value.base, 8);
    }
    else
      append_as_byte_string((*iter).second->accum, value, value_len);

    if ((*iter).second->accum.fill() > m_server_flush_limit)
      m_full = true;
    m_memory_used += incr_mem;
  }
}


void TableMutatorAsyncScatterBuffer::set_delete(const Key &key, size_t incr_mem) {
  ScopedLock lock(m_mutex);

  RangeLocationInfo range_info;
  TableMutatorAsyncSendBufferMap::const_iterator iter;

  if (key.flag == FLAG_INSERT)
    HT_THROW(Error::BAD_KEY, "Key flag is FLAG_INSERT, expected delete");

  if (!m_location_cache->lookup(m_table_identifier.id, key.row, &range_info)) {
    Timer timer(m_timeout_ms, true);
    m_range_locator->find_loop(&m_table_identifier, key.row, &range_info,
                               timer, false);
  }
  iter = m_buffer_map.find(range_info.addr);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.addr] = new TableMutatorAsyncSendBuffer(&m_table_identifier,
        &m_completion_counter, m_range_locator.get());
    iter = m_buffer_map.find(range_info.addr);
    (*iter).second->addr = range_info.addr;
  }

  (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
  if (key.flag == FLAG_DELETE_COLUMN_FAMILY ||
      key.flag == FLAG_DELETE_CELL || key.flag == FLAG_DELETE_CELL_VERSION) {
    if (key.column_family_code == 0)
      HT_THROWF(Error::BAD_KEY, "key.flag set to %d but column family=0", key.flag);
    if (key.flag == FLAG_DELETE_CELL || key.flag == FLAG_DELETE_CELL_VERSION) {
      if (key.flag == FLAG_DELETE_CELL_VERSION && key.timestamp == AUTO_ASSIGN) {
        HT_THROWF(Error::BAD_KEY, "key.flag set to %d but timestamp == AUTO_ASSIGN", key.flag);
      }
    }
  }

  create_key_and_append((*iter).second->accum, key.flag, key.row,
      key.column_family_code, key.column_qualifier, key.timestamp);
  append_as_byte_string((*iter).second->accum, 0, 0);
  if ((*iter).second->accum.fill() > m_server_flush_limit)
    m_full = true;
  m_memory_used += incr_mem;
}


void
TableMutatorAsyncScatterBuffer::set(SerializedKey key, ByteString value, size_t incr_mem) {
  ScopedLock lock(m_mutex);

  RangeLocationInfo range_info;
  TableMutatorAsyncSendBufferMap::const_iterator iter;
  const uint8_t *ptr = key.ptr;
  size_t len = Serialization::decode_vi32(&ptr);

  if (!m_location_cache->lookup(m_table_identifier.id, (const char *)ptr+1,
                                &range_info)) {
    Timer timer(m_timeout_ms, true);
    m_range_locator->find_loop(&m_table_identifier, (const char *)ptr+1,
                               &range_info, timer, false);
  }

  iter = m_buffer_map.find(range_info.addr);

  if (iter == m_buffer_map.end()) {
    m_buffer_map[range_info.addr] = new TableMutatorAsyncSendBuffer(&m_table_identifier,
                                                                    &m_completion_counter, m_range_locator.get());
    iter = m_buffer_map.find(range_info.addr);
    (*iter).second->addr = range_info.addr;
  }

  (*iter).second->key_offsets.push_back((*iter).second->accum.fill());
  (*iter).second->accum.add(key.ptr, (ptr-key.ptr)+len);
  (*iter).second->accum.add(value.ptr, value.length());

  if ((*iter).second->accum.fill() > m_server_flush_limit)
    m_full = true;
  m_memory_used += incr_mem;
}


namespace {

  struct SendRec {
    SerializedKey key;
    uint64_t offset;
  };

  inline bool operator<(const SendRec &sr1, const SendRec &sr2) {
    const char *row1 = sr1.key.row();
    const char *row2 = sr2.key.row();
    int rval = strcmp(row1, row2);
    if (rval == 0)
      return sr1.offset < sr2.offset;
    return rval < 0;
  }
}


void TableMutatorAsyncScatterBuffer::send(uint32_t flags) {
  ScopedLock lock(m_mutex);
  bool outstanding=false;

  m_timer.start();
  TableMutatorAsyncSendBufferPtr send_buffer;
  std::vector<SendRec> send_vec;
  uint8_t *ptr;
  SerializedKey key;
  SendRec send_rec;
  size_t len;
  String range_location;

  HT_ASSERT(!m_outstanding);
  m_completion_counter.set(m_buffer_map.size());

  for (TableMutatorAsyncSendBufferMap::const_iterator iter = m_buffer_map.begin();
       iter != m_buffer_map.end(); ++iter) {
    send_buffer = (*iter).second;

    if ((len = send_buffer->accum.fill()) == 0) {
      m_completion_counter.decrement();
      continue;
    }

    send_buffer->pending_updates.set(new uint8_t [len], len);

    if (send_buffer->resend()) {
      memcpy(send_buffer->pending_updates.base,
             send_buffer->accum.base, len);
      send_buffer->send_count = send_buffer->retry_count;
    }
    else {
      send_vec.clear();
      send_vec.reserve(send_buffer->key_offsets.size());
      for (size_t i=0; i<send_buffer->key_offsets.size(); i++) {
        send_rec.key.ptr = send_buffer->accum.base
          + send_buffer->key_offsets[i];
        send_rec.offset = send_buffer->key_offsets[i];
        send_vec.push_back(send_rec);
      }
      sort(send_vec.begin(), send_vec.end());

      ptr = send_buffer->pending_updates.base;

      for (size_t i=0; i<send_vec.size(); i++) {
        key = send_vec[i].key;
        key.next();  // skip key
        key.next();  // skip value
        memcpy(ptr, send_vec[i].key.ptr, key.ptr - send_vec[i].key.ptr);
        ptr += key.ptr - send_vec[i].key.ptr;
      }
      HT_ASSERT((size_t)(ptr-send_buffer->pending_updates.base)==len);
      send_buffer->dispatch_handler =
        new TableMutatorAsyncDispatchHandler(m_app_queue, m_mutator, m_id, send_buffer.get(),
                                             m_auto_refresh);
      send_buffer->send_count = send_buffer->key_offsets.size();
    }

    send_buffer->accum.free();
    send_buffer->key_offsets.clear();

    /**
     * Send update
     */
    try {
      m_send_flags = flags;
      send_buffer->pending_updates.own = false;
      m_range_server.update(send_buffer->addr, ClusterId::get(),
                            m_table_identifier, send_buffer->send_count,
                            send_buffer->pending_updates, flags,
                            send_buffer->dispatch_handler.get());

      outstanding = true;

      if (flags & Table::MUTATOR_FLAG_NO_LOG_SYNC)
        m_unsynced_rangeservers.insert(send_buffer->addr);

    }
    catch (Exception &e) {
      if (e.code() == Error::COMM_NOT_CONNECTED ||
          e.code() == Error::COMM_BROKEN_CONNECTION ||
          e.code() == Error::COMM_INVALID_PROXY) {
        m_range_locator->invalidate_host(send_buffer->addr.proxy);
        send_buffer->add_retries(send_buffer->send_count, 0,
                                 send_buffer->pending_updates.size);
        if (e.code() == Error::COMM_NOT_CONNECTED ||
            e.code() == Error::COMM_INVALID_PROXY)
          m_completion_counter.decrement();
        else
          outstanding = true;
        // Random wait between 0 and 5 seconds
        poll(0, 0, m_rng()%5000);
      }
      else {
        HT_FATALF("Problem sending updates to %s - %s (%s)",
                  send_buffer->addr.to_str().c_str(), Error::get_text(e.code()),
                  e.what());
      }
    }
    send_buffer->pending_updates.own = true;
  }

  if (outstanding)
    m_outstanding = true;
  else
    m_app_queue->add_unlocked(new TableMutatorAsyncHandler(m_mutator, m_id));
}


void TableMutatorAsyncScatterBuffer::wait_for_completion() {
  ScopedLock lock(m_mutex);

  while(m_outstanding)
    m_cond.wait(lock);
}


TableMutatorAsyncScatterBuffer *
TableMutatorAsyncScatterBuffer::create_redo_buffer(uint32_t id) {
  TableMutatorAsyncSendBufferPtr send_buffer;
  TableMutatorAsyncScatterBuffer *redo_buffer = 0;
  SerializedKey key;
  ByteString value, bs;
  size_t incr_mem;

  try {
    if (m_timer.remaining() < m_wait_time) {
      set_retries_to_fail(Error::REQUEST_TIMEOUT);
      HT_THROW(Error::REQUEST_TIMEOUT,
               format("Timer remaining=%lld wait_time=%lld",
                      (Lld)m_timer.remaining(), (Lld)m_wait_time));
    }

    m_timer.start();
    poll(0,0, m_wait_time);
    m_timer.stop();
    redo_buffer = new TableMutatorAsyncScatterBuffer(m_comm, m_app_queue, m_mutator,
        &m_table_identifier, m_schema, m_range_locator, m_auto_refresh, m_timeout_ms, id);
    redo_buffer->m_timer = m_timer;
    redo_buffer->m_wait_time = m_wait_time + 2000;

    for (TableMutatorAsyncSendBufferMap::const_iterator iter = m_buffer_map.begin();
        iter != m_buffer_map.end(); ++iter) {
      send_buffer = (*iter).second;

      if (send_buffer->accum.fill()) {
        const uint8_t *endptr;

        bs.ptr = send_buffer->accum.base;
        endptr = bs.ptr + send_buffer->accum.fill();

        // now add all of the old keys to the redo buffer
        while (bs.ptr < endptr) {
          key.ptr = bs.next();
          value.ptr = bs.next();
          incr_mem = key.length() + value.length();
          redo_buffer->set(key, value, incr_mem);
          m_resends++;
        }
      }
    }
  }
  catch (Exception &e) {
    if (redo_buffer != 0) {
      delete redo_buffer;
      redo_buffer=0;
    }
    set_retries_to_fail(e.code());
    HT_THROW(e.code(), e.what());
  }
  return redo_buffer;
}

void TableMutatorAsyncScatterBuffer::set_retries_to_fail(int error) {
  TableMutatorAsyncSendBufferPtr send_buffer;
  Key key;
  Cell cell;
  ByteString value, bs;
  Schema::ColumnFamily *cf;

  for (TableMutatorAsyncSendBufferMap::const_iterator iter = m_buffer_map.begin();
      iter != m_buffer_map.end(); ++iter) {
    send_buffer = (*iter).second;
    if (send_buffer->accum.fill()) {
      const uint8_t *endptr;

      bs.ptr = send_buffer->accum.base;
      endptr = bs.ptr + send_buffer->accum.fill();

      while (bs.ptr < endptr) {
        key.load((SerializedKey)bs);
        cell.row_key = key.row;
        cell.flag = key.flag;
        if (cell.flag == FLAG_DELETE_ROW) {
          cell.column_family = 0;
        }
        else {
          cf = m_schema->get_column_family(key.column_family_code);
          HT_ASSERT(cf);
          cell.column_family = m_constant_strings.get(cf->name.c_str());
        }
        cell.column_qualifier = key.column_qualifier;
        cell.timestamp = key.timestamp;
        bs.next();
        cell.value_len = bs.decode_length(&cell.value);
        bs.next();
        m_failed_mutations.push_back(std::make_pair(cell, error));
      }
    }
  }
}

int TableMutatorAsyncScatterBuffer::set_failed_mutations() {
  TableMutatorAsyncSendBufferPtr send_buffer;
  std::vector<FailedRegionAsync> failed_regions;
  int error = Error::OK;

  for (TableMutatorAsyncSendBufferMap::const_iterator it = m_buffer_map.begin();
      it != m_buffer_map.end(); ++it) {
    (*it).second->get_failed_regions(failed_regions);
    (*it).second->failed_regions.clear();
  }

  if (!failed_regions.empty()) {
    Cell cell;
    Key key;
    ByteString bs;
    const uint8_t *endptr;
    Schema::ColumnFamily *cf;
    for (size_t i=0; i<failed_regions.size(); i++) {
      bs.ptr = failed_regions[i].base;
      endptr = bs.ptr + failed_regions[i].len;
      while (bs.ptr < endptr) {
        key.load((SerializedKey)bs);
        cell.row_key = key.row;
        cell.flag = key.flag;
        if (cell.flag == FLAG_DELETE_ROW) {
          cell.column_family = 0;
        }
        else {
          cf = m_schema->get_column_family(key.column_family_code);
          HT_ASSERT(cf);
          cell.column_family = m_constant_strings.get(cf->name.c_str());
        }
        cell.column_qualifier = key.column_qualifier;
        cell.timestamp = key.timestamp;
        bs.next();
        cell.value_len = bs.decode_length(&cell.value);
        bs.next();
        m_failed_mutations.push_back(std::make_pair(cell,
              failed_regions[i].error));
      }
    }
  }
  if (m_failed_mutations.size() > 0)
    error = failed_regions[0].error;
  return error;
}

void TableMutatorAsyncScatterBuffer::finish() {

  int error = Error::OK;
  bool has_retries=false;

  if (m_completion_counter.has_errors()) {
    error = set_failed_mutations();
    // this prevents this mutation failure logic from being executed twice
    // if this method gets called again
    m_completion_counter.clear_errors();
  }

  if (m_completion_counter.has_retries()) {
    has_retries = true;
  }

  m_mutator->buffer_finish(m_id, error, has_retries);

  {
    ScopedLock lock(m_mutex);
    m_outstanding = false;
    m_cond.notify_all();
  }
}
