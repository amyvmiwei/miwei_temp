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
 * %RangeServer %MetaLog entity type constants.
 * This file contains MetaLog entity type constants for the %RangeServer
 */

#ifndef HYPERTABLE_RANGESERVERMETALOGENTITYTYPES_H
#define HYPERTABLE_RANGESERVERMETALOGENTITYTYPES_H

namespace Hypertable {

  /** @addtogroup libHypertable
   *  @{
   */

  namespace MetaLog {

    /** %MetaLog entity type constants */
    namespace EntityType {

      /** %RangeServer %MetaLog entity type constants */
      enum RangeServerTypes {
        RANGE                       = 0x00010001,
        RANGE2                      = 0x00010002,
        TASK_REMOVE_TRANSFER_LOG    = 0x00010003,
        TASK_ACKNOWLEDGE_RELINQUISH = 0x00010004,
        REMOVE_OK_LOGS              = 0x00010005
      };
    }
  }

  /* @}*/
}

#endif // HYPERTABLE_RANGESERVERMETALOGENTITYTYPES_H
