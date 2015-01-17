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

/// @file
/// Declarations for Protocol.
/// This file contains declarations for Protocol, a class encapsulating
/// information about the Master-client protocol.

#ifndef Hypertable_Lib_Master_Protocol_h
#define Hypertable_Lib_Master_Protocol_h

namespace Hypertable {
namespace Lib {
namespace Master {

  /// @addtogroup libHypertableMaster
  /// @{

  class Protocol {
  public:
    enum {
      COMMAND_CREATE_TABLE = 0,
      COMMAND_GET_SCHEMA,
      COMMAND_STATUS,
      COMMAND_REGISTER_SERVER,
      COMMAND_MOVE_RANGE,
      COMMAND_DROP_TABLE,
      COMMAND_ALTER_TABLE,
      COMMAND_SHUTDOWN,
      COMMAND_CLOSE,
      COMMAND_CREATE_NAMESPACE,
      COMMAND_DROP_NAMESPACE,
      COMMAND_RENAME_TABLE,
      COMMAND_RELINQUISH_ACKNOWLEDGE,
      COMMAND_FETCH_RESULT,
      COMMAND_BALANCE,
      COMMAND_REPLAY_COMPLETE,
      COMMAND_PHANTOM_PREPARE_COMPLETE,
      COMMAND_PHANTOM_COMMIT_COMPLETE,
      COMMAND_STOP,
      COMMAND_REPLAY_STATUS,
      COMMAND_COMPACT,
      COMMAND_SET_STATE,
      COMMAND_RECREATE_INDEX_TABLES,
      COMMAND_MAX
    };
  };

  /// @}

}}}

#endif // Hypertable_Lib_Master_Protocol_h
