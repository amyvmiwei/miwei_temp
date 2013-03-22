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
 * Crontab class for periodic events.
 * The Crontab class is used to track the timing of a periodic event.
 * This is used i.e. in the Hypertable.Master to schedule LoadBalancer events.
 */

#ifndef HYPERTABLE_CRONTAB_H
#define HYPERTABLE_CRONTAB_H

#include "ReferenceCount.h"
#include "String.h"

#include <time.h>

#include <bitset>

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  struct crontab_entry {
    std::bitset<60> minute;
    std::bitset<24> hour;
    std::bitset<31> dom;
    std::bitset<12> month;
    std::bitset<8>  dow;
  };

  /**
   * The Crontab class is used to track the timing of a
   * periodic event.  It is constructed with a string argument
   * that has the format of first five fields (date/time fields)
   * of a crontab entry and is used to determine at what time
   * an event should occcur.
   */
  class Crontab : public ReferenceCount {
  public:
    /** Constructor initializes the Crontab from a string which is formatted
     * like a regular unix crontab entry:
     *
     *   "1 1 * * * *"
     *   "1-2/2/4 1 * * *"
     *   "2-1 1 * * *"
     *   "1-8/ 1 * * *"
     *
     * see tests/crontab_test.cc for more examples.
     *
     * @param spec The crontab string describing the time interval
     * @throws Error::COMMAND_PARSE_ERROR if the spec cannot be parsed
     */
    Crontab(const String &spec);

    /** Retrieves the timestamp of the next event.
     *
     * @param now The current time, in seconds since the epoch
     * @return The absolute time of the next event, in seconds since
     *      the epoch */
    time_t next_event(time_t now);

    /** Get the internal crontab_entry structure; required for testing */
    crontab_entry &entry() { return m_entry; }

  private:
    /** Helper function for next_event(); returns the day when the
     * next event occurs
     */
    void next_matching_day(struct tm *next_tm, bool increment);

    /** Parses a crontab spec into a crontab_entry */
    void parse_entry(const String &spec, crontab_entry *_entry);

    /** Parses a string into a number and asserts that it fits into the 
     * range [0..N[ (or [1..N] if zero_based is false)
     */ 
    template <int N> void parse_range(const String &spec, std::bitset<N> &bits,
            bool zero_based = true);

    crontab_entry m_entry;
  };

  typedef boost::intrusive_ptr<Crontab> CrontabPtr;

  /** helper to print a crontab_entry */
  std::ostream &operator<<(std::ostream &os, const crontab_entry &entry);

  /** helper function to recreate a crontab spec from the parsed entries;
   * used for testing
   */
  template <int N> void reconstruct_spec(const std::bitset<N> &bits,
            String &spec);

  /** @}*/

} // namespace Hypertable

#endif // HYPERTABLE_CRONTAB_H
