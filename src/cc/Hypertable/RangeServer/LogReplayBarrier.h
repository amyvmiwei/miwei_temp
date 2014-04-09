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

/// @file
/// Declarations for LogReplayBarrier.
/// This file contains type declarations for LogReplayBarrier, a class used
/// to block requests until required commit log has finished replaying.

#ifndef Hypertable_RangeServer_LogReplayBarrier_h
#define Hypertable_RangeServer_LogReplayBarrier_h

#include <Hypertable/Lib/Types.h>

#include <Common/Mutex.h>

#include <boost/thread/condition.hpp>
#include <boost/thread/xtime.hpp>

#include <memory>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Blocks requests until specific commit log has finished replaying.
  /// Many operations require a certain class of ranges to be online before they
  /// can proceed.  All ranges belong to one of the four following classes:
  ///   - ROOT
  ///   - METADATA
  ///   - SYSTEM
  ///   - USER
  ///
  /// When the RangeServer starts up, all of the ranges in a given class are
  /// brought online at the same time.  Each range class has an associated
  /// commit log and once the commit log has been replayed, the ranges are
  /// "flipped live".  This class provides a mechanism for operations to block
  /// until commit log replay for dependent ranges is complete (and are
  /// therefore live).  The range classes are flipped live (i.e. finished log
  /// replay) in the order in which they're listed above.
  class LogReplayBarrier {
  public:

    /// Signals ROOT commit log replay has been completed.
    /// Sets #m_root_complete to <i>true</i> and signals #m_root_complete_cond
    void set_root_complete();

    /// Signals METADATA commit log replay has been completed.
    /// Sets #m_metadata_complete to <i>true</i> and signals
    /// #m_metadata_complete_cond
    void set_metadata_complete();

    /// Signals SYSTEM commit log replay has been completed.
    /// Sets #m_system_complete to <i>true</i> and signals
    /// #m_system_complete_cond
    void set_system_complete();

    /// Signals USER commit log replay has been completed.
    /// Sets #m_user_complete to <i>true</i> and signals #m_user_complete_cond
    void set_user_complete();

    /// Waits for ROOT commit log replay to complete.
    /// Performs a timed wait on #m_root_complete_cond with
    /// <code>deadline</code> passed in as the timeout value.
    /// @param deadline Absolute time representing wait deadline
    /// @return <i>true</i> if ROOT commit log replay completed before
    /// <code>deadline</code>, <i>false</i> if <code>deadline</code> reached
    /// before ROOT commit log replay completed.
    bool wait_for_root(boost::xtime deadline);

    /// Waits for METADATA commit log replay to complete.
    /// Performs a timed wait on #m_metadata_complete_cond with
    /// <code>deadline</code> passed in as the timeout value.
    /// @param deadline Absolute time representing wait deadline
    /// @return <i>true</i> if METADATA commit log replay completed before
    /// <code>deadline</code>, <i>false</i> if <code>deadline</code> reached
    /// before METADATA commit log replay completed.
    bool wait_for_metadata(boost::xtime deadline);

    /// Waits for SYSTEM commit log replay to complete.
    /// Performs a timed wait on #m_system_complete_cond with
    /// <code>deadline</code> passed in as the timeout value.
    /// @param deadline Absolute time representing wait deadline
    /// @return <i>true</i> if SYSTEM commit log replay completed before
    /// <code>deadline</code>, <i>false</i> if <code>deadline</code> reached
    /// before SYSTEM commit log replay completed.
    bool wait_for_system(boost::xtime deadline);

    /// Waits for USER commit log replay to complete.
    /// Performs a timed wait on #m_user_complete_cond with
    /// <code>deadline</code> passed in as the timeout value.
    /// @param deadline Absolute time representing wait deadline
    /// @return <i>true</i> if USER commit log replay completed before
    /// <code>deadline</code>, <i>false</i> if <code>deadline</code> reached
    /// before USER commit log replay completed.
    bool wait_for_user(boost::xtime deadline);

    /// Waits for commit log replay to complete for range class defined by a
    /// given range.
    /// This member function will perform a timed wait on the condition variable
    /// for the range class to which the given range belongs, defined by
    /// <code>table</code> and <code>range</code>.  <code>deadline</code> is
    /// passed in as the timeout value.
    /// @param deadline Absolute time representing wait deadline
    /// @param table %Table identifier
    /// @param range %Range specification
    /// @return <i>true</i> if commit log replay completed before
    /// <code>deadline</code>, <i>false</i> if <code>deadline</code> reached
    /// before USER commit log replay completed.
    bool wait(boost::xtime deadline,
              const TableIdentifier *table, const RangeSpec *range=0);

    /// Checks if replay of USER commit log is complete
    /// @return <i>true</i> if USER commit log replay is complete, <i>false</i>
    /// otherwise
    bool user_complete();

  private:
    /// %Mutex to serialize concurrent access.
    Mutex m_mutex;
    /// Condition variable used to signal ROOT commit log replay complete.
    boost::condition m_root_complete_cond;
    /// Condition variable used to signal METADATA commit log replay complete.
    boost::condition m_metadata_complete_cond;
    /// Condition variable used to signal SYSTEM commit log replay complete.
    boost::condition m_system_complete_cond;
    /// Condition variable used to signal USER commit log replay complete.
    boost::condition m_user_complete_cond;
    /// Flag indicating if ROOT commit log replay is complete
    bool m_root_complete {};
    /// Flag indicating if METADATA commit log replay is complete
    bool m_metadata_complete {};
    /// Flag indicating if SYSTEM commit log replay is complete
    bool m_system_complete {};
    /// Flag indicating if USER commit log replay is complete
    bool m_user_complete {};
  };

  /// Smart pointer to LogReplayBarrier
  typedef std::shared_ptr<LogReplayBarrier> LogReplayBarrierPtr;

  /// @}

} // namespace Hypertable

#endif // Hypertable_RangeServer_LogReplayBarrier_h
