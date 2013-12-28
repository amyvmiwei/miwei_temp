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
/// Crontab class for periodic events.
/// The Crontab class is used to track the timing of a periodic event.
/// This is used i.e. in the Hypertable.Master to schedule LoadBalancer events.

#ifndef HYPERTABLE_CRONTAB_H
#define HYPERTABLE_CRONTAB_H

#include "String.h"

#include <time.h>

#include <bitset>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Binary representation of crontab spec.
  struct crontab_entry {
    std::bitset<60> minute;
    std::bitset<24> hour;
    std::bitset<31> dom;
    std::bitset<12> month;
    std::bitset<8>  dow;
  };

  /// Tracks timing of periodic events.
  /// The Crontab class is used to track the timing of a
  /// periodic event.  It is constructed with a string argument
  /// that has the format of first five fields (date/time fields)
  /// of a crontab entry and is used to determine at what time
  /// an event should occcur.
  class Crontab {
  public:

    /// Default constructor.
    Crontab() { };

    /// Constructor initialized with crontab(5) entry.
    /// initializes the Crontab from a string which is formatted
    /// like a regular unix crontab entry:
    /// <pre>
    ///   "1 1 * * * *"
    ///   "1-2/2/4 1 * * *"
    ///   "2-1 1 * * *"
    ///   "1-8/ 1 * * *"
    /// <pre>
    /// @see <code>Common/tests/crontab_test.cc</code> for more examples.
    /// @param spec The crontab string describing the time interval
    /// @throws Error::COMMAND_PARSE_ERROR if the spec cannot be parsed
    Crontab(const String &spec);

    /// Retrieves the timestamp of the next event.
    /// @param now The current time, in seconds since the epoch
    /// @return The absolute time of the next event, in seconds since
    ///      the epoch
    time_t next_event(time_t now);

    /// Get the internal crontab_entry structure.
    /// @note Required for testing
    crontab_entry &entry() { return m_entry; }

  private:

    /// Determines next day on which event will occur.
    /// Using the date passed in through <code>next_tm</code> as the starting
    /// date, modifies it to hold the day in which the next event will occur.
    /// The <code>tm_hour</code>, <code>tm_min</code>, and <code>tm_sec</code>
    /// fields are cleared.
    /// @param next_tm Structure holding starting date and modified to hold the
    ///   next day the event will occur
    /// @param increment Increment <code>next_tm</code> by one day on entry to
    /// function
    void next_matching_day(struct tm *next_tm, bool increment);

    /// Parses a crontab spec into a crontab_entry.
    /// @param spec %Crontab spec
    /// @param _entry Binary crontab specification filled in by function
    void parse_entry(const String &spec, crontab_entry *_entry);

    /// Parses a crontab field and sets corresponding bits in <code>bits</code>.
    /// @tparam N Sizeof  <code>bits</code>
    /// @param spec %Crontab field
    /// @param bits Bit set to populate
    /// @param zero_based Indicates is crontab field is zero or one based.
    template <int N> void parse_range(const String &spec, std::bitset<N> &bits,
            bool zero_based = true);

    crontab_entry m_entry;
  };

  /// Helper function to write crontab_entry to an ostream.
  /// @param os Output stream
  /// @param entry Binary crontab specification structure
  /// @return Output stream (<code>os</code>)
  std::ostream &operator<<(std::ostream &os, const crontab_entry &entry);

  /// Converts binary crontab spec back into string spec.
  /// @note For testing
  /// @tparam N Number of bits in <code>bits</code>
  /// @param bits Bit set representing crontab field
  /// @param spec %Crontab field string equivalent of <code>bits</code>
  template <int N> void reconstruct_spec(const std::bitset<N> &bits,
            String &spec);

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_CRONTAB_H
