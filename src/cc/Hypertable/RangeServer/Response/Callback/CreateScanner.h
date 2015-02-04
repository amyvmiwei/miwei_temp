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

#ifndef Hypertable_RangeServer_Response_Callback_CreateScanner_h
#define Hypertable_RangeServer_Response_Callback_CreateScanner_h

#include <Hypertable/Lib/ProfileDataScanner.h>

#include <AsyncComm/ResponseCallback.h>

#include <boost/shared_array.hpp>

namespace Hypertable {
namespace RangeServer {
namespace Response {
namespace Callback {

  /// @addtogroup RangeServerResponseCallback
  /// @{

  class CreateScanner : public ResponseCallback {
  public:
    CreateScanner(Comm *comm, EventPtr &event)
      : ResponseCallback(comm, event) { }

    int response(int32_t id, int32_t skipped_rows, int32_t skipped_cells,
                 bool more, ProfileDataScanner &profile_data,
                 StaticBuffer &ext);

    int response(int32_t id, int32_t skipped_rows, int32_t skipped_cells,
                 bool more, ProfileDataScanner &profile_data,
                 boost::shared_array<uint8_t> &ext_buffer, uint32_t ext_len);
  };

  /// @}

}}}}


#endif // Hypertable_RangeServer_Response_Callback_CreateScanner_h
