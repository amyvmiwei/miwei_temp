/** -*- c++ -*-
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_TABLEMUTATORQUEUE_H
#define HYPERTABLE_TABLEMUTATORQUEUE_H

#include <list>
#include <boost/thread/condition.hpp>

#include "Common/Thread.h"
#include "Common/Mutex.h"
#include "AsyncComm/ApplicationQueueInterface.h"

#include "ScanCells.h"

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
    TableMutatorQueue(Mutex &mutex, boost::condition &cond) : m_mutex(mutex), m_cond(cond) { }

    ~TableMutatorQueue () { }

    /**
     */
    virtual void add(ApplicationHandler *app_handler) {
      ScopedLock lock(m_mutex);
      add_unlocked(app_handler);
    }

    virtual void add_unlocked(ApplicationHandler *app_handler) {
      m_work_queue.push_back(app_handler);
      m_cond.notify_one();
    }

    void wait_for_buffer(ScopedLock &lock, ApplicationHandler **app_handlerp) {
      {
        while (m_work_queue.empty()) {
          m_cond.wait(lock);
        }
        *app_handlerp = m_work_queue.front();
        HT_ASSERT(*app_handlerp);
        m_work_queue.pop_front();
      }
    }

  private:

    typedef std::list<ApplicationHandler *> WorkQueue;
    Mutex                  &m_mutex;
    boost::condition       &m_cond;
    WorkQueue              m_work_queue;
  };

  typedef boost::intrusive_ptr<TableMutatorQueue> TableMutatorQueuePtr;

} // namespace Hypertable

#endif // HYPERTABLE_TABLESCANNERQUEUE_H
