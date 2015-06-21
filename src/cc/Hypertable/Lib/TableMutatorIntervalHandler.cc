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

/** @file
 * Definitions for TableMutatorIntervalHandler.
 * This file contains definitions for TableMutatorIntervalHandler, a class that
 * is used as the timer handler for periodically flushing a shared mutator.
 */

#include "Common/Compat.h"
#include "Common/md5.h"

#include "AsyncComm/Comm.h"

#include "HyperAppHelper/Unique.h"

#include "TableMutatorShared.h"
#include "TableMutatorIntervalHandler.h"
#include "TableMutatorFlushHandler.h"

using namespace Hypertable;

TableMutatorIntervalHandler::TableMutatorIntervalHandler(Comm *comm,
					 ApplicationQueueInterface *app_queue,
					 TableMutatorShared *shared_mutator)
  : active(true), complete(false), comm(comm), app_queue(app_queue), shared_mutator(shared_mutator) {
}

void TableMutatorIntervalHandler::start() {
  char unique_hash[33];
  uint32_t first_interval;
  string str = HyperAppHelper::generate_guid();

  md5_string(str.c_str(), unique_hash);

  memcpy(&first_interval, unique_hash, 4);

  first_interval %= shared_mutator->flush_interval();

  HT_ASSERT(comm->set_timer(first_interval, shared_from_this()) == Error::OK);
}


void TableMutatorIntervalHandler::handle(EventPtr &event) {

  if (active) {
    TableMutatorIntervalHandlerPtr handler = static_pointer_cast<TableMutatorIntervalHandler>(shared_from_this());
    app_queue->add(new TableMutatorFlushHandler(handler, event));
    HT_ASSERT(comm->set_timer(shared_mutator->flush_interval(), shared_from_this()) == Error::OK);
  }
  else {
    std::lock_guard<std::mutex> lock(mutex);
    complete = true;
    cond.notify_all();
  }
}


void TableMutatorIntervalHandler::flush() {
  std::lock_guard<std::mutex> lock(mutex);

  if (active)
    shared_mutator->interval_flush();
}
