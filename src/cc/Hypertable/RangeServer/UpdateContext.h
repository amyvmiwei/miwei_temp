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

#ifndef Hypertable_RangeServer_UpdateContext_h
#define Hypertable_RangeServer_UpdateContext_h

#include <Hypertable/RangeServer/UpdateRecRange.h>
#include <Hypertable/RangeServer/UpdateRecTable.h>

#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Context record for update request passed into UpdatePipeline.
  class UpdateContext {
  public:

    /// Constructor.
    /// @param updates Vector of updates
    /// @param xt Expiration time
    UpdateContext(std::vector<UpdateRecTable *> &updates, boost::xtime xt) :
      updates(updates), expire_time(xt) { }

    /// Destructor.
    ~UpdateContext() {
      for (auto u : updates)
        delete u;
    }
    std::vector<UpdateRecTable *> updates;
    boost::xtime expire_time;
    int64_t auto_revision;
    SendBackRec send_back;
    DynamicBuffer root_buf;
    int64_t last_revision;
    uint32_t total_updates {};
    uint32_t total_added {};
    uint32_t total_syncs {};
    uint64_t total_bytes_added {};
  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_RangeServer_UpdateContext_h
