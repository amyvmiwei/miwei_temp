/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#ifndef Hypertable_RangeServer_ResponseCallbackCreateScanner_h
#define Hypertable_RangeServer_ResponseCallbackCreateScanner_h

#include <Hypertable/Lib/ProfileDataScanner.h>

#include <AsyncComm/ResponseCallback.h>

#include <boost/shared_array.hpp>

namespace Hypertable {

  class ResponseCallbackCreateScanner : public ResponseCallback {
  public:
    ResponseCallbackCreateScanner(Comm *comm, EventPtr &event)
      : ResponseCallback(comm, event) { }

    int response(short moreflag, int32_t id, StaticBuffer &ext,
                 int32_t skipped_rows, int32_t skipped_cells,
                 ProfileDataScanner &profile_data);

    int response(short moreflag, int32_t id, 
                 boost::shared_array<uint8_t> &ext_buffer, uint32_t ext_len,
                 int32_t skipped_rows, int32_t skipped_cells,
                 ProfileDataScanner &profile_data);
  };

}


#endif // Hypertable_RangeServer_ResponseCallbackCreateScanner_h
