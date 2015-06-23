/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef Hypertable_Lib_TableMutatorShared_h
#define Hypertable_Lib_TableMutatorShared_h

#include "TableMutator.h"

#include <chrono>
#include <memory>
#include <mutex>

namespace Hypertable {

class TableMutatorIntervalHandler;

/**
 * A TableMutator that can be shared from multiple threads and incidentally
 * has an option to do periodic flushes. For best throughput use the vanilla
 * TableMutator
 */
class TableMutatorShared : public TableMutator {
  typedef TableMutator Parent;

public:
  /**
   * @param props smart pointer to the Comm layer
   * @param comm pointer to the Comm layer
   * @param table pointer to the table object
   * @param range_locator smart pointer to the range locator
   * @param app_queue pointer to the application queue
   * @param timeout_ms maximum time in milliseconds to allow methods
   *        to execute before throwing an exception
   * @param flush_interval_ms period in milliseconds to flush
   * @param flags rangeserver client update command flags
   */
  TableMutatorShared(PropertiesPtr &props, Comm *comm, Table *table,
      RangeLocatorPtr &range_locator, ApplicationQueueInterfacePtr &app_queue,
      uint32_t timeout_ms, uint32_t flush_interval_ms, uint32_t flags = 0);

  virtual ~TableMutatorShared();

  /**
   * @see TableMutator::set
   */
  virtual void set(const KeySpec &key, const void *value, uint32_t value_len) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    Parent::set(key, value, value_len);
  }

  /**
   * @see TableMutator::set_delete
   */
  virtual void set_delete(const KeySpec &key) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    Parent::set_delete(key);
  }

  /**
   * @see TableMutator::set_cells
   */
  virtual void set_cells(const Cells &cells) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    Parent::set_cells(cells);
  }

  /**
   * @see TableMutator::flush
   */
  virtual void flush() {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    Parent::flush();
    m_last_flush_ts = std::chrono::steady_clock::now();
  }

  /**
   * @see TableMutator::retry
   */
  virtual bool retry(uint32_t timeout_ms = 0) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    return Parent::retry(timeout_ms);
  }

  /**
   * @see TableMutator:memory_used
   */
  virtual uint64_t memory_used() {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    return Parent::memory_used();
  }

  /**
   * @see TableMutator::get_resend_count
   */
  virtual uint64_t get_resend_count() {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    return Parent::get_resend_count();
  }

  /**
   * @see TableMutator::get_failed
   */
  virtual void get_failed(FailedMutations &failed_mutations) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    return Parent::get_failed(failed_mutations);
  }

  /**
   * @see TableMutator::need_retry
   */
  virtual bool need_retry() {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    return Parent::need_retry();
  }

  uint32_t flush_interval() { return m_flush_interval; }

  /**
   * Flush if necessary considering the flush interval
   */
  void interval_flush();

private:
  std::recursive_mutex m_mutex;
  uint32_t m_flush_interval;
  std::chrono::steady_clock::time_point m_last_flush_ts;
  std::shared_ptr<TableMutatorIntervalHandler> m_tick_handler;
};

}

#endif /* Hypertable_Lib_TableMutatorShared_h */
