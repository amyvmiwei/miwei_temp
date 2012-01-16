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

#ifndef HYPERTABLE_REMOVALMANAGER_H
#define HYPERTABLE_REMOVALMANAGER_H

#include "Common/HashMap.h"

#include "Hypertable/Lib/MetaLogWriter.h"

#include "Operation.h"

#include <list>

#include <boost/thread/condition.hpp>

namespace Hypertable {

  class RemovalManagerContext {
  public:
    RemovalManagerContext() : shutdown(false) { }
    class RemovalRec {
    public:
      RemovalRec() : approvals_remaining(0) { }
      RemovalRec(Operation *_op, size_t needed_approvals)
        : op(_op), approvals_remaining(needed_approvals) { }
      OperationPtr op;
      size_t approvals_remaining;
    };
    typedef hash_map<int64_t, RemovalRec> RemovalMapT;
    Mutex mutex;
    boost::condition cond;
    MetaLog::WriterPtr mml_writer;
    RemovalMapT map;
    std::list<OperationPtr> removal_queue;
    bool shutdown;
  };

  /**
   * RemovalManager is a class that is used manage explicit removal operations.
   * Most operations do not require explicit removal and will get removed
   * automatically upon completion.  However some operations, such as the
   * MOVE_RANGE operation, must be explicitly removed only after some condition
   * is met (e.g. relinquish acknowledge).
   */
  class RemovalManager {
  public:
    RemovalManager(MetaLog::WriterPtr &mml_writer);
    void operator()();
    void shutdown();
    bool add_operation(OperationPtr &operation) { return add_operation(operation.get()); }
    bool add_operation(Operation *operation);
    void approve_removal(int64_t hash_code);
    void approve_removal(OperationPtr &operation) { approve_removal(operation->hash_code()); }

  private:
    RemovalManagerContext *m_ctx;
    Thread *m_thread;
  };


} // namespace Hypertable

#endif // HYPERTABLE_REMOVALMANAGER_H
