/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_FUTURE_H
#define HYPERTABLE_FUTURE_H

#include <boost/thread/condition.hpp>
#include <list>
#include <map>

#include "ResultCallback.h"
#include "Result.h"

namespace Hypertable {

  using namespace std;
  class Future : public ResultCallback {
  public:

    /**
     * Future objects are used to access results from asynchronous scanners/mutators
     * @param capacity byte capacity of future queue. Results will be blocked from being
     *     enqueued if the amount of memory used by the existing enqueued results exceeds
     *     this amount. Defaults to zero, making the queue capacity unbounded.
     */
    Future(size_t capacity=0) : m_capacity(capacity), m_memory_used(0), m_cancelled(false)
        { }
    ~Future() { cancel(); }

    /**
     * This call blocks till there is a result available unless async ops have completed
     * @param result will contain a reference to the result object
     * @return true if asynchronous operations have completed
     */
    bool get(ResultPtr &result);

    /**
     * This call blocks for the lesser of timeout / time till there is a result available
     * @param result will contain a reference to the result object
     * @param timeout_ms max milliseconds to block for
     * @param timed_out set to true if the call times out
     * @return false if asynchronous operations have completed
     */
    bool get(ResultPtr &result, uint32_t timeout_ms, bool &timed_out);

    /**
     * Cancels outstanding scanners/mutators. Callers responsibility to make sure that
     * this method gets called before async scanner/mutator destruction when the application
     * abruptly stops processing async results before all operations are complete
     */
    void cancel();

    void register_scanner(TableScannerAsync *scanner);

    void deregister_scanner(TableScannerAsync *scanner);

    void register_mutator(TableMutatorAsync *scanner);

    void deregister_mutator(TableMutatorAsync *scanner);

    /**
     * Checks whether the Future result queue is full
     */
    bool is_full() {
      ScopedLock lock(m_outstanding_mutex);
      return !has_remaining_capacity();
    }

    /**
     * Checks whether the Future result queue is empty
     */
    bool is_empty() {
      ScopedLock lock(m_outstanding_mutex);
      return _is_empty();
    }

    /**
     * Checks whether the Future object has been cancelled
     */
    bool is_cancelled() {
      ScopedLock lock(m_outstanding_mutex);
      return _is_cancelled();
    }

    /**
     * Checks whether there are any outstanding operations
     */
    bool has_outstanding() {
      return !is_done();
    }

  private:
    friend class TableScannerAsync;
    friend class TableMutator;
    typedef list<ResultPtr> ResultQueue;

    void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells);
    void scan_error(TableScannerAsync *scanner, int error, const String &error_msg,
                    bool eos);
    void update_ok(TableMutatorAsync *mutator);
    void update_error(TableMutatorAsync *mutator, int error, FailedMutations &failures);

    size_t memory_used() {
      return m_memory_used;
    }

    bool has_remaining_capacity() {
      if (!m_capacity)
        // unbounded buffer
        return true;
      else
        return m_memory_used < m_capacity;
    }

    bool _is_empty() { return m_queue.empty(); }
    bool _is_cancelled() const {
      return m_cancelled;
    }

    bool _is_done() {
      return m_outstanding == 0;
    }

    void enqueue(ResultPtr &result);

    ResultQueue m_queue;
    size_t m_capacity;
    size_t m_memory_used;
    bool m_cancelled;
    typedef map<uint64_t, TableScannerAsync *> ScannerMap;
    typedef map<uint64_t, TableMutatorAsync *> MutatorMap;
    ScannerMap m_scanner_map;
    MutatorMap m_mutator_map;
  };
  typedef intrusive_ptr<Future> FuturePtr;
}

#endif // HYPERTABLE_FUTURE_H
