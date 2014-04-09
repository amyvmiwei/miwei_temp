/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Declarations for UpdateRequest.
/// This file contains type declarations for UpdateRequest, a class representing
/// a client update request.

#ifndef Hypertable_RangeServer_UpdateRequest_h
#define Hypertable_RangeServer_UpdateRequest_h

#include <AsyncComm/Event.h>

#include <Common/StaticBuffer.h>

#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Describes portion of an update buffer rejected due to error.
  struct SendBackRec {
    /// Error code
    int error;
    /// Number of key/value pairs to which #error applies
    uint32_t count;
    /// Starting byte offset within update buffer of rejected key/value pairs
    uint32_t offset; 
    /// Length (in bytes) from #offset covering key/value pairs rejected
    uint32_t len;
  };

  /// Holds client update request and error state.
  class UpdateRequest {
  public:
    /// Update buffer containing serialized key/value pairs
    StaticBuffer buffer;
    /// Count of serialized key/value pairs in #buffer
    uint32_t count {};
    /// Event object of originating update requst
    EventPtr event;
    /// Vector of SendBacRec objects describing rejected key/value pairs
    std::vector<SendBackRec> send_back_vector;
    /// Error code that applies to entire buffer
    uint32_t error {};
  };

  /// @}
}

#endif // Hypertable_RangeServer_UpdateRequest_h
