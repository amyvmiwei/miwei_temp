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

/** @file
 * Declarations for RangeServerProtocol.
 * This file contains declarations for RangeServerProtocol, a class for
 * generating RangeServer protocol messages.
 */

#ifndef Hypertable_Lib_RangeServer_Protocol_h
#define Hypertable_Lib_RangeServer_Protocol_h

#include <Common/StaticBuffer.h>

#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/RangeServerRecovery/ReceiverPlan.h>
#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/RangeState.h>
#include <Hypertable/Lib/ScanSpec.h>
#include <Hypertable/Lib/SystemVariable.h>
#include <Hypertable/Lib/TableIdentifier.h>

namespace Hypertable {
namespace Lib {
namespace RangeServer {

  using namespace std;

  /// @addtogroup libHypertable
  /// @{

  /// Master client protocol information
  class Protocol {
  public:
    enum {
      COMMAND_LOAD_RANGE = 0,
      COMMAND_UPDATE,
      COMMAND_CREATE_SCANNER,
      COMMAND_FETCH_SCANBLOCK,
      COMMAND_COMPACT,
      COMMAND_STATUS,
      COMMAND_SHUTDOWN,
      COMMAND_DUMP,
      COMMAND_DESTROY_SCANNER,
      COMMAND_DROP_TABLE,
      COMMAND_DROP_RANGE,
      COMMAND_REPLAY_BEGIN,
      COMMAND_REPLAY_LOAD_RANGE,
      COMMAND_REPLAY_UPDATE,
      COMMAND_REPLAY_COMMIT,
      COMMAND_GET_STATISTICS,
      COMMAND_UPDATE_SCHEMA,
      COMMAND_COMMIT_LOG_SYNC,
      COMMAND_CLOSE,
      COMMAND_WAIT_FOR_MAINTENANCE,
      COMMAND_ACKNOWLEDGE_LOAD,
      COMMAND_RELINQUISH_RANGE,
      COMMAND_HEAPCHECK,
      COMMAND_METADATA_SYNC,
      COMMAND_INITIALIZE,
      COMMAND_REPLAY_FRAGMENTS,
      COMMAND_PHANTOM_LOAD,
      COMMAND_PHANTOM_UPDATE,
      COMMAND_PHANTOM_PREPARE_RANGES,
      COMMAND_PHANTOM_COMMIT_RANGES,
      COMMAND_DUMP_PSEUDO_TABLE,
      COMMAND_SET_STATE,
      COMMAND_TABLE_MAINTENANCE_ENABLE,
      COMMAND_TABLE_MAINTENANCE_DISABLE,
      COMMAND_MAX
    };

    enum RangeGroup {
      GROUP_METADATA_ROOT,
      GROUP_METADATA,
      GROUP_SYSTEM,
      GROUP_USER
    };

    // The flags shd be the same as in Hypertable::TableMutator.
    enum {
      /* Don't force a commit log sync on update */
      UPDATE_FLAG_NO_LOG_SYNC        = 0x0001,
      UPDATE_FLAG_NO_LOG             = 0x0004
    };

    // Compaction flags
    enum CompactionFlags {
      COMPACT_FLAG_ROOT     = 0x0001,
      COMPACT_FLAG_METADATA = 0x0002,
      COMPACT_FLAG_SYSTEM   = 0x0004,
      COMPACT_FLAG_USER     = 0x0008,
      COMPACT_FLAG_ALL      = 0x000F,
      COMPACT_FLAG_MINOR    = 0x0010,
      COMPACT_FLAG_MAJOR    = 0x0020,
      COMPACT_FLAG_MERGING  = 0x0040,
      COMPACT_FLAG_GC       = 0x0080
    };

    static string compact_flags_to_string(uint32_t flags);

  };

  /// @}
}}}

#endif // Hypertable_Lib_RangeServer_Protocol_h
