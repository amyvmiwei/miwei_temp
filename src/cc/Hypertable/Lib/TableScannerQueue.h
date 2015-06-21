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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef Hypertable_Lib_TableScannerQueue_h
#define Hypertable_Lib_TableScannerQueue_h

#include "ScanCells.h"

#include <AsyncComm/ApplicationQueueInterface.h>

#include <condition_variable>
#include <list>
#include <mutex>

namespace Hypertable {

  /**
   * Provides application work queue and worker threads.  It maintains a queue
   * of requests and a pool of threads that pull requests off the queue and
   * carry them out.
   */
  class TableScannerQueue : public ApplicationQueueInterface {

  public:

    /** Default constructor.
     */
    TableScannerQueue() { }

    ~TableScannerQueue () { }

    /**
     */
    virtual void add(ApplicationHandler *app_handler) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_work_queue.push_back(app_handler);
      m_cond.notify_one();
    }

    virtual void add_unlocked(ApplicationHandler *app_handler) { }

    void next_result(ScanCellsPtr &cells, int *error, std::string &error_msg) {
      ApplicationHandler *app_handler;
      cells = 0;
      *error = Error::OK;
      while(true) {
        {
          std::unique_lock<std::mutex> lock(m_mutex);
          if (m_error != Error::OK && !m_error_shown) {
            *error = m_error;
            error_msg = m_error_msg;
            m_error_shown = true;
            break;
          }
          else if (!m_cells_queue.empty()) {
            cells = m_cells_queue.front();
            m_cells_queue.pop_front();
            break;
          }
          m_cond.wait(lock, [this](){
              return !m_work_queue.empty() || !m_cells_queue.empty(); });
          if (!m_work_queue.size())
            continue;
          app_handler = m_work_queue.front();
          HT_ASSERT(app_handler);
          m_work_queue.pop_front();
        }
        app_handler->run();
        delete app_handler;
      }
      if (m_error != Error::OK) {
        *error = m_error;
        error_msg = m_error_msg;
        cells = 0;
      }
      HT_ASSERT(cells != 0 || *error != Error::OK);
    }

    void add_cells(ScanCellsPtr &cells) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_cells_queue.push_back(cells);
      m_cond.notify_one();
    }

    void set_error(int error, const std::string &error_msg) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_error = error;
      m_error_msg = error_msg;
      m_error_shown = false;
    }

  private:

    typedef std::list<ApplicationHandler *> WorkQueue;
    typedef std::list<ScanCellsPtr> CellsQueue;
    std::mutex m_mutex;
    std::condition_variable m_cond;
    WorkQueue m_work_queue;
    CellsQueue m_cells_queue;
    std::string m_error_msg;
    int m_error {};
    bool m_error_shown {};
  };

  /// Shared smart pointer to TableScannerQueue
  typedef std::shared_ptr<TableScannerQueue> TableScannerQueuePtr;

}

#endif // Hypertable_Lib_TableScannerQueue_h
