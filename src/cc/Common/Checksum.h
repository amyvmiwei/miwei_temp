/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

/** @file
 * Implementation of checksum routines.
 * This file implements the fletcher32 checksum algorithm.
 */

#ifndef HYPERTABLE_CHECKSUM_H
#define HYPERTABLE_CHECKSUM_H

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /** Compute fletcher32 checksum for arbitary data. See
   * http://en.wikipedia.org/wiki/Fletcher%27s_checksum for more information
   * about the algorithm. Fletcher32 is the default checksum used in Hypertable.
   *
   * @param data Pointer to the input data
   * @param len Input data length in bytes
   * @return The calculated checksum
   */
  extern uint32_t fletcher32(const void *data, size_t len);

  /** @}*/

} // namespace Hypertable

#endif /* HYPERTABLE_CHECKSUM_H */
