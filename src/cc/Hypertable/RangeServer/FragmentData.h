/* -*- c++ -*-
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

/// @file
/// Declarations for FragmentData.
/// This file contains the type declarations for FragmentData, a class for
/// accumulating phantom update data for a phantom range.

#ifndef HYPERTABLE_FRAGMENTDATA_H
#define HYPERTABLE_FRAGMENTDATA_H

#include <Hypertable/RangeServer/Range.h>
#include <Hypertable/RangeServer/ScanContext.h>

#include <Hypertable/Lib/CommitLog.h>
#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/Types.h>

#include <AsyncComm/Event.h>

#include <Common/ByteString.h>
#include <Common/Mutex.h>
#include <Common/ReferenceCount.h>
#include <Common/ReferenceCount.h>
#include <Common/atomic.h>

#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /** Accumulates phantom update data for a phantom range.
   * The recovery of a range server involves the <i>phantom</i> loading of its
   * ranges into new range servers and then replaying its commit log to
   * reconstruct the in-memory state of each recovered range.  This replaying of
   * the commit log involves reading each commit log file fragment and sending
   * the commits found within the fragment, to the range servers that hold the
   * phantom ranges to which they apply, by calling
   * RangeServer::phantom_update().  Once the commit log replay is complete, a
   * <i>transfer log</i> will be created for each phantom range to contain this
   * commit log data.  This transfer log is associated with each phantom range
   * so that once they get flipped live, any subsequent restart of the range
   * server can reconstruct the in-memory state for the range by replaying the
   * transfer log. This class really exists as just an optimization to avoid
   * copying the data received via RangeServer::phantom_update().  It does this
   * in the following two ways:
   *   - Accumulates the Event objects that represent calls to
   *     RangeServer::phantom_update()
   *   - Decodes the Event objects and adds the data which they contain to the
   *     corresponding phantom range and the range's newly created transfer
   *     log (see merge())
   */
  class FragmentData : public ReferenceCount {
  public:

    /** Constructor.
     * Initializes #m_memory_consumption to 0.
     */
    FragmentData() : m_memory_consumption(0) {}

    /** Destructor.
     * Subtracts #m_memory_consumption from global memory tracker
     */
    virtual ~FragmentData();

    /** Adds an Event object representing a call to RangeServer::phantom_update().
     * Adds <code>event</code> to #m_data and adds the amount of memory consumed
     * by <code>event</code> to both the global memory tracker and
     * #m_memory_consumption.
     * @param event RangeServer::phantom_update() event
     */
    void add(EventPtr &event);

    /** Clears the state.
     * Clears the #m_data vector, subtracts #m_memory_consumption from global
     * memory tracker, and then sets #m_memory_consumption to 0.
     */
    void clear();

    /** Adds accumulated data to phantom range and its transfer log.
     * For each accumulated event object, decodes the data held within
     * the object (see RangeServerProtocol::create_request_phantom_update() for
     * encoding format) and adds the data to the phantom range
     * <code>range</code> and also writes the data to the phantom range's
     * transfer log <code>log</code>.
     * @param table %Table identifier of phantom range
     * @param range Pointer to phantom range object
     * @param log Phantom range's transfer log
     * @see RangeServerProtocol::create_request_phantom_update()
     */
    void merge(TableIdentifier &table, RangePtr &range, CommitLogPtr &log);

  private:

    /// Vector of RangeServer::phantom_update() events
    vector<EventPtr> m_data;

    /// Amount of memory accumulated (for memory tracking)
    int64_t m_memory_consumption;
  };

  /// Smart pointer to FragmentData
  typedef boost::intrusive_ptr<FragmentData> FragmentDataPtr;

  /// @}
}

#endif // HYPERTABLE_FRAGMENTDATA_H
