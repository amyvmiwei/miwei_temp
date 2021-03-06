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

#ifndef Hyperspace_response_ResponseCallbackStatus_h
#define Hyperspace_response_ResponseCallbackStatus_h

#include <AsyncComm/CommBuf.h>
#include <AsyncComm/ResponseCallback.h>

#include <Common/Error.h>
#include <Common/Status.h>

#include <string>

namespace Hyperspace {

  class ResponseCallbackStatus : public Hypertable::ResponseCallback {
  public:
    ResponseCallbackStatus(Hypertable::Comm *comm,
                           Hypertable::EventPtr &event_ptr)
      : Hypertable::ResponseCallback(comm, event_ptr) { }

    int response(const Hypertable::Status &status);
  };

}

#endif // Hyperspace_response_ResponseCallbackStatus_h
