/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
 * %Master %MetaLog entity type constants.
 * This file contains MetaLog entity type constants for the %Master
 */

#ifndef HYPERTABLE_MASTERMETALOGENTITYTYPES_H
#define HYPERTABLE_MASTERMETALOGENTITYTYPES_H

namespace Hypertable {

  /** @addtogroup libHypertable
   *  @{
   */

  namespace MetaLog {

    /** %MetaLog entity type constants */
    namespace EntityType {

      /** %Master %MetaLog entity type constants */
      enum MasterTypes {

        RANGE_SERVER_CONNECTION              = 0x00020000,

        OLD_OPERATION_TEST                   = 0x00020001,
        OLD_OPERATION_STATUS                 = 0x00020002,
        OLD_OPERATION_SYSTEM_UPGRADE         = 0x00020003,
        OLD_OPERATION_INITIALIZE             = 0x00020004,
        OLD_OPERATION_COLLECT_GARBAGE        = 0x00020005,
        OLD_OPERATION_GATHER_STATISTICS      = 0x00020006,
        OLD_OPERATION_WAIT_FOR_SERVERS       = 0x00020007,
        OLD_OPERATION_REGISTER_SERVER        = 0x00020008,
        OLD_OPERATION_RECOVER_SERVER         = 0x00020009,
        OLD_OPERATION_CREATE_NAMESPACE       = 0x0002000A,
        OLD_OPERATION_DROP_NAMESPACE         = 0x0002000B,
        OLD_OPERATION_CREATE_TABLE           = 0x0002000C,
        OLD_OPERATION_DROP_TABLE             = 0x0002000D,
        OLD_OPERATION_ALTER_TABLE            = 0x0002000E,
        OLD_OPERATION_RENAME_TABLE           = 0x0002000F,
        OLD_OPERATION_GET_SCHEMA             = 0x00020010,
        OLD_OPERATION_MOVE_RANGE             = 0x00020011,
        OLD_OPERATION_RELINQUISH_ACKNOWLEDGE = 0x00020012,
        OLD_OPERATION_BALANCE                = 0x00020013,
        OLD_OPERATION_LOAD_BALANCER          = 0x00020014,

        BALANCE_PLAN_AUTHORITY               = 0x00030000,

        OPERATION_TEST                       = 0x00030001,
        OPERATION_STATUS                     = 0x00030002,
        OPERATION_SYSTEM_UPGRADE             = 0x00030003,
        OPERATION_INITIALIZE                 = 0x00030004,
        OPERATION_COLLECT_GARBAGE            = 0x00030005,
        OPERATION_GATHER_STATISTICS          = 0x00030006,
        OPERATION_WAIT_FOR_SERVERS           = 0x00030007,
        OPERATION_REGISTER_SERVER            = 0x00030008,
        OPERATION_RECOVER_SERVER             = 0x00030009,
        OPERATION_CREATE_NAMESPACE           = 0x0003000A,
        OPERATION_DROP_NAMESPACE             = 0x0003000B,
        OPERATION_CREATE_TABLE               = 0x0003000C,
        OPERATION_DROP_TABLE                 = 0x0003000D,
        OPERATION_ALTER_TABLE                = 0x0003000E,
        OPERATION_RENAME_TABLE               = 0x0003000F,
        OPERATION_GET_SCHEMA                 = 0x00030010,
        OPERATION_MOVE_RANGE                 = 0x00030011,
        OPERATION_RELINQUISH_ACKNOWLEDGE     = 0x00030012,
        OPERATION_BALANCE_RETIRED            = 0x00030013,
        OPERATION_LOAD_BALANCER_RETIRED      = 0x00030014,
        OPERATION_RECOVER_SERVER_RANGES      = 0x00030015,
        OPERATION_RECOVERY_BLOCKER           = 0x00030016,
        OPERATION_STOP                       = 0x00030017,
        OPERATION_BALANCE                    = 0x00030018,
        OPERATION_TIMED_BARRIER              = 0x00030019,
        OPERATION_COMPACT                    = 0x0003001A,
        OPERATION_SET                        = 0x0003001B,
        SYSTEM_STATE                         = 0x0003001C,
        OPERATION_REGISTER_SERVER_BLOCKER    = 0x0003001D,
        OPERATION_TOGGLE_TABLE_MAINTENANCE   = 0x0003001E,
        OPERATION_RECREATE_INDEX_TABLES      = 0x0003001F
      };
    }
  }

  /* @}*/
}

#endif // HYPERTABLE_MASTERMETALOGENTITYTYPES_H
