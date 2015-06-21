/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#ifndef Hypertable_Lib_ResultCallback_h
#define Hypertable_Lib_ResultCallback_h

#include "ClientObject.h"
#include "ScanCells.h"

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace Hypertable {

  using namespace Lib;

  class TableScannerAsync;
  class TableMutatorAsync;

  /** Represents an open table.
   */
  class ResultCallback : public ClientObject {

  public:

    ResultCallback() { }

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
    virtual void scan_error(TableScannerAsync *scanner, int error, const std::string &error_msg,
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
      std::unique_lock<std::mutex> lock(m_outstanding_mutex);
      m_outstanding_cond.wait(lock, [this](){ return m_outstanding == 0; });
    }

    /**
     *
     */
    void increment_outstanding() {
      std::lock_guard<std::mutex> lock(m_outstanding_mutex);
      m_outstanding++;
    }

    /**
     *
     */
    void decrement_outstanding() {
      std::lock_guard<std::mutex> lock(m_outstanding_mutex);
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
      std::lock_guard<std::mutex> lock(m_outstanding_mutex);
      return m_outstanding == 0;
    }

  protected:
    int m_outstanding {};
    std::mutex m_outstanding_mutex;
    std::condition_variable m_outstanding_cond;
  };
 
  /// Shared smart pointer to ResultCallback
  typedef std::shared_ptr<ResultCallback> ResultCallbackPtr;

}

#endif // Hypertable_Lib_ResultCallback_h
