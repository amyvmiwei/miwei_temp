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

#include "Common/Compat.h"
#include "Global.h"
#include "MaintenanceTaskWorkQueue.h"

using namespace Hypertable;


/**
 *
 */
MaintenanceTaskWorkQueue::MaintenanceTaskWorkQueue(int level, int priority,
						   std::vector<MetaLog::EntityTaskPtr> &work)
  : MaintenanceTask(level, priority, String("WORK QUEUE")) {
  m_work.swap(work);
}


MaintenanceTaskWorkQueue::~MaintenanceTaskWorkQueue() {
  if (!m_completed.empty())
    Global::rsml_writer->record_removal(m_completed);
}


/**
 *
 */
void MaintenanceTaskWorkQueue::execute() {
  foreach_ht (MetaLog::EntityTaskPtr &entity_task, m_work) {
    try {
      if (!entity_task->execute()) {
	ScopedLock lock(Global::mutex);
	Global::work_queue.push_back(entity_task);
      }
      else
	m_completed.push_back(entity_task.get());
    }
    catch (Hypertable::Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      ScopedLock lock(Global::mutex);
      Global::work_queue.push_back(entity_task);
    }
    catch (std::exception &e) {
      HT_ERROR_OUT << "Problem executing " << entity_task->name() << " - " << e.what() << HT_END;
      ScopedLock lock(Global::mutex);
      Global::work_queue.push_back(entity_task);
    }
  }
}
