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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * Helper class for printing usage banners on the command line.
 */

#ifndef HYPERTABLE_USAGE_H
#define HYPERTABLE_USAGE_H

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * Helper class for printing usage banners on the command line
   */
  class Usage {
  public:
    /**
     * Prints each string in %usage to std::cout, delimited by std::endl;
     * %usage must be terminated with a NULL pointer
     */
    static void dump(const char **usage);

    /**
     * Same as %dump, but performs _exit(rcode) afterwards
     */
    static void dump_and_exit(const char **usage, int rcode = 1);
  };

  /** @} */
}

#endif // HYPERTABLE_USAGE_H
