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

#ifndef HYPERSPACE_GROUPCOMMITTIMERHANDLER_H
#define HYPERSPACE_GROUPCOMMITTIMERHANDLER_H

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/DispatchHandler.h"

namespace Hypertable {

  class RangeServer;

  /**
   */
  class GroupCommitTimerHandler : public DispatchHandler {

  public:
    GroupCommitTimerHandler(Comm *comm, RangeServer *range_server, ApplicationQueuePtr &app_queue);
    virtual void handle(Hypertable::EventPtr &event_ptr);
    void shutdown();

    void wait_for_shutdown() {
      ScopedLock lock(m_mutex);
      while (!m_shutdown_complete)
        m_shutdown_cond.wait(lock);
    }

  private:
    Mutex         m_mutex;
    Comm         *m_comm;
    RangeServer  *m_range_server;
    ApplicationQueuePtr m_app_queue;
    int32_t       m_commit_interval;
    boost::condition m_shutdown_cond;
    bool          m_shutdown;
    bool          m_shutdown_complete;
  };
  typedef boost::intrusive_ptr<GroupCommitTimerHandler> GroupCommitTimerHandlerPtr;
}

#endif // HYPERSPACE_GROUPCOMMITTIMERHANDLER_H

