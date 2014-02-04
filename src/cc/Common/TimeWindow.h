/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
/// Declarations for TimeWindow.
/// This file contains declarations for TimeWindow, a class for defining a
/// set of time windows and checking if the current time falls with any of
/// those windows.

#ifndef Common_TimeWindow_h
#define Common_TimeWindow_h

#include <Common/Crontab.h>

#include <vector>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Defines a time window.
  /// This class can be used to define a time window (or set of discontiguous
  /// time windows) that can be used, for example, to define times when certain
  /// activities can take place.  It can be initialized with a set of
  /// <i>crontab</i> entries that define the time window(s) and has member
  /// functions for setting the current time and testing to see if the current
  /// time falls within the defined time window(s).  The motivating use case
  /// for this class was to allow the %Hypertable %RangeServers to define the
  /// daily period of low activity and have it change it's maintenance algorithm
  /// to be more aggressive during these periods.  Example usage:
  /// <pre>
  /// vector<String> specs;
  /// specs.push_back("* 2-5 * * *");
  /// %TimeWindow low_activity_time(specs);
  /// ...
  /// time_t now = time(0);
  /// low_activity_time.set_current_time(now);
  /// if (low_activity_time.within_window()) {
  ///   ... perform heavy mainteance activity ...
  /// }
  /// </pre>
  class TimeWindow {
  public:

    /// Default constructor.
    TimeWindow() : m_within_window(false), m_enabled(true) { }

    /// Constructor initialized with crontab specs.
    /// This function constructs a Crontab object with each of the specs in
    /// <code>crontab_specs</code> and pushes them onto #m_crontabs.  It wraps
    /// each Crontab object construction in a try/catch block and asserts with a
    /// message indicating the bad entry that caused the constructor to throw an
    /// exception.
    /// @param crontab_specs %Crontab specs that define the time window(s)
    TimeWindow(std::vector<String> crontab_specs);

    /// Logically sets the current time.
    /// This function fetches the current time (or uses <code>now</code> if
    /// non-zero) and determines if it falls within the time window by checking
    /// it against all of the Crontab objects in #m_crontabs.  For each Crontab
    /// object in #m_crontabs, it calls Crontab::next_event() and if any of them
    /// report that the next event is within 60 seconds of the current time, it
    /// sets #m_within_window to <i>true</i>, otherwise it sets it to
    /// <i>false</i>.
    /// @param now Time to use for checking if we're within the time window
    /// @return <i>true</i> if updating the current time has caused an entry
    /// or exit from the window, <i>false</i> otherwise.
    bool update_current_time(time_t now=0);

    /// Tests to see if current time is within the window.
    /// Checks to see if the current time (as set by set_current_time()) is
    /// within the time window.
    /// @return <i>true</i> if within enabled time window, <i>false</i> otherwise
    bool within_window() const { return m_within_window && m_enabled; }

    /// Enables or disables this time window.
    /// @param value <i>true</i> if enabled, <i>false</i> otherwise
    void enable_window(bool enable) { m_enabled = enable; }

    /// Indicates if the time window is enabled or disabled.
    /// @return <i>true</i> if enabled, <i>false</i> otherwise
    bool is_window_enabled() const { return m_enabled; }

  private:

    /// Vector of Crontab objects that define each subwindow
    std::vector<Crontab> m_crontabs;

    /// Set to <i>true</i> if currently within the time window
    bool m_within_window;

    /// Set to <i>true</i> if the time window is enabled
    bool m_enabled;
  };

  /// @}
}

#endif // Common_TimeWindow_h
