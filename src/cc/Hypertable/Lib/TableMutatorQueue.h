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

#ifndef Hypertable_Lib_TableMutatorQueue_h
#define Hypertable_Lib_TableMutatorQueue_h

#include "ScanCells.h"

#include <AsyncComm/ApplicationQueueInterface.h>

#include <Common/Thread.h>

#include <condition_variable>
#include <list>
#include <mutex>

namespace Hypertable {

  /**
   * Provides application work queue and worker threads.  It maintains a queue
   * of requests and a pool of threads that pull requests off the queue and
   * carry them out.
   */
  class TableMutatorQueue : public ApplicationQueueInterface {

  public:

    /**
     *
     */
    TableMutatorQueue(std::mutex &mutex, std::condition_variable &cond) : m_mutex(mutex), m_cond(cond) { }

    ~TableMutatorQueue () { }

    /**
     */
    virtual void add(ApplicationHandler *app_handler) {
      std::lock_guard<std::mutex> lock(m_mutex);
      add_unlocked(app_handler);
    }

    virtual void add_unlocked(ApplicationHandler *app_handler) {
      m_work_queue.push_back(app_handler);
      m_cond.notify_one();
    }

    void wait_for_buffer(std::unique_lock<std::mutex> &lock, ApplicationHandler **app_handlerp) {
      m_cond.wait(lock, [this](){ return !m_work_queue.empty();});
      *app_handlerp = m_work_queue.front();
      HT_ASSERT(*app_handlerp);
      m_work_queue.pop_front();
    }

  private:

    typedef std::list<ApplicationHandler *> WorkQueue;
    std::mutex &m_mutex;
    std::condition_variable &m_cond;
    WorkQueue m_work_queue;
  };

  /// Shared smart pointer to TableMutatorQueue
  typedef std::shared_ptr<TableMutatorQueue> TableMutatorQueuePtr;

}

#endif // Hypertable_Lib_TableMutatorQueue_h
