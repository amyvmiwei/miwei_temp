/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
#ifndef HYPERTABLE_RANGESET_H
#define HYPERTABLE_RANGESET_H

#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

namespace Hypertable {

  /**
   * Interface for removing a range or changing its end row in a Range set.
   */
  class RangeSet : public ReferenceCount {
  public:

    /**
     * Removes the range associated with the given end_row
     *
     * @param start_row end row of range to remove
     * @param end_row end row of range to remove
     * @return true if removed, false if not found
     */
    virtual bool remove(const String &start_row, const String &end_row) = 0;

    /**
     * Changes the end row key associated with a range
     *
     * @param start_row start row of range to remove
     * @param old_end_row old end row key of range
     * @param new_end_row new end row key for range
     * @return true if range found and end row changed, false otherwise
     */
    virtual bool change_end_row(const String &start_row,
                                const String &old_end_row, const String &new_end_row) = 0;

    /**
     * Changes the start row key associated with a range
     *
     * @param old_start_row Old start row of range to remove
     * @param new_start_row New start row key of range
     * @param new_end_row New end row key for range
     * @return true if range found and end row changed, false otherwise
     */
    virtual bool change_start_row(const String &old_start_row, const String &new_start_row,
                                  const String &new_end_row) = 0;

  };

  typedef intrusive_ptr<RangeSet> RangeSetPtr;

}

#endif // HYPERTABLE_RANGESET_H
