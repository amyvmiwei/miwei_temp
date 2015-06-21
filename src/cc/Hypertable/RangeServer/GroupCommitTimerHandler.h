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

#ifndef Hypertable_RangeServer_GroupCommitTimerHandler_h
#define Hypertable_RangeServer_GroupCommitTimerHandler_h

#include <AsyncComm/ApplicationQueue.h>
#include <AsyncComm/Comm.h>
#include <AsyncComm/DispatchHandler.h>

#include <memory>
#include <mutex>

namespace Hypertable {

  namespace Apps {
    class RangeServer;
  }

  class GroupCommitTimerHandler : public DispatchHandler {

  public:
    GroupCommitTimerHandler(Comm *comm, Apps::RangeServer *range_server, ApplicationQueuePtr &app_queue);
    void start();
    virtual void handle(Hypertable::EventPtr &event_ptr);
    void shutdown();

  private:
    std::mutex m_mutex;
    Comm *m_comm {};
    Apps::RangeServer *m_range_server {};
    ApplicationQueuePtr m_app_queue;
    int32_t m_commit_interval {};
    bool m_shutdown {};
  };

  /// Shared smart pointer to GroupCommitTimerHandler
  typedef std::shared_ptr<GroupCommitTimerHandler> GroupCommitTimerHandlerPtr;
}

#endif // Hypertable_RangeServer_GroupCommitTimerHandler_h

