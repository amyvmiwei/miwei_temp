/*
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
#include <Common/Compat.h>

#include "GroupCommitTimerHandler.h"
#include "Request/Handler/GroupCommit.h"

#include <Common/Config.h>
#include <Common/Error.h>
#include <Common/InetAddr.h>
#include <Common/StringExt.h>
#include <Common/System.h>
#include <Common/Time.h>

using namespace Hypertable;
using namespace Hypertable::RangeServer;
using namespace Hypertable::Config;
using namespace std;

GroupCommitTimerHandler::GroupCommitTimerHandler(Comm *comm, Apps::RangeServer *range_server,
                                                 ApplicationQueuePtr &app_queue) 
  : m_comm(comm), m_range_server(range_server), m_app_queue(app_queue) {
  m_commit_interval = get_i32("Hypertable.RangeServer.CommitInterval");
}

void GroupCommitTimerHandler::start() {
  int error;
  if ((error = m_comm->set_timer(m_commit_interval, shared_from_this())) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));
}


void GroupCommitTimerHandler::handle(Hypertable::EventPtr &event_ptr) {
  lock_guard<mutex> lock(m_mutex);
  int error;

  if (m_shutdown)
    return;

  m_app_queue->add( new Request::Handler::GroupCommit(m_range_server) );

  if ((error = m_comm->set_timer(m_commit_interval, shared_from_this())) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));
}


void GroupCommitTimerHandler::shutdown() {
  lock_guard<mutex> lock(m_mutex);
  m_shutdown = true;
  m_comm->cancel_timer(shared_from_this());
}
