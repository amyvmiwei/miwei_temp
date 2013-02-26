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

#ifndef HYPERTABLE_RESULTCALLBACKINTERFACE_H
#define HYPERTABLE_RESULTCALLBACKINTERFACE_H

#include <vector>
#include <map>
#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "ScanCells.h"
namespace Hypertable {

  class TableScannerAsync;
  class TableMutatorAsync;

  /** Represents an open table.
   */
  class ResultCallback: public ReferenceCount {

  public:

    ResultCallback() : m_outstanding(0) { }

    virtual ~ResultCallback() {
      wait_for_completion();
    }

    /**
     * Hook for derived classes which want to keep track of scanners/mutators
     */
    virtual void register_scanner(TableScannerAsync *scanner) { }

    /**
     * Hook for derived classes which want to keep track of scanners/mutators
     */
    virtual void deregister_scanner(TableScannerAsync *scanner) { }

    /**
     * Hook for derived classes which want to keep track of scanners/mutators
     */
    virtual void register_mutator(TableMutatorAsync *mutator) { }

    /**
     * Hook for derived classes which want to keep track of scanners/mutators
     */
    virtual void deregister_mutator(TableMutatorAsync *mutator) { }

    /**
     * Callback method for completion, default one does nothing.
     *
     */
    virtual void completed() { }

    /**
     * Callback method for successful scan
     *
     * @param scanner
     * @param cells returned cells
     */
    virtual void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells)=0;

    /**
     * Callback method for scan errors
     *
     * @param scanner
     * @param error
     * @param error_msg
     * @param eos end of scan
     */
    virtual void scan_error(TableScannerAsync *scanner, int error, const String &error_msg,
                            bool eos)=0;

    /**
     * Callback method for successful update
     *
     * @param mutator
     */
    virtual void update_ok(TableMutatorAsync *mutator)=0;

    /**
     * Callback method for update errors
     *
     * @param mutator Mutator pointer
     * @param error error code
     * @param failures vector of failed mutations
     */
    virtual void update_error(TableMutatorAsync *mutator, int error, FailedMutations &failures)=0;

    /**
     * Blocks till outstanding == 0
     */
    void wait_for_completion() {
      ScopedLock lock(m_outstanding_mutex);
      while (m_outstanding)
        m_outstanding_cond.wait(lock);
    }

    /**
     *
     */
    void increment_outstanding() {
      ScopedLock lock(m_outstanding_mutex);
      m_outstanding++;
    }

    /**
     *
     */
    void decrement_outstanding() {
      ScopedLock lock(m_outstanding_mutex);
      HT_ASSERT(m_outstanding > 0);
      m_outstanding--;
      if (m_outstanding == 0) {
        completed();
        m_outstanding_cond.notify_all();
      }
    }

    /**
     *
     */
    bool is_done() {
      ScopedLock lock(m_outstanding_mutex);
      return m_outstanding == 0;
    }

  protected:
    int m_outstanding;
    Mutex m_outstanding_mutex;
    boost::condition m_outstanding_cond;
  };
  typedef intrusive_ptr<ResultCallback> ResultCallbackPtr;
} // namespace Hypertable

#endif // HYPERTABLE_RESULTCALLBACKINTERFACE_H
