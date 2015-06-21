/*
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

#ifndef Hyperspace_ClientHandleState_h
#define Hyperspace_ClientHandleState_h

#include "HandleCallback.h"
#include "LockSequencer.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

namespace Hyperspace {

  class ClientHandleState {
  public:
    uint64_t     handle;
    uint32_t     open_flags;
    uint32_t     event_mask;
    std::string  normal_name;
    HandleCallbackPtr callback;
    LockSequencer *sequencer;
    int lock_status;
    uint32_t lock_mode;
    uint64_t lock_generation;
    std::mutex mutex;
    std::condition_variable cond;
  };
  typedef std::shared_ptr<ClientHandleState> ClientHandleStatePtr;

}

#endif // Hyperspace_ClientHandleState_h
