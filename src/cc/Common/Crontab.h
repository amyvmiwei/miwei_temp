/**
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
#ifndef HYPERTABLE_CRONTAB_H
#define HYPERTABLE_CRONTAB_H

#include "ReferenceCount.h"
#include "String.h"

#include <time.h>

#include <bitset>

namespace Hypertable {

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
    Crontab(const String &spec);
    time_t next_event(time_t now);
    crontab_entry &entry() { return m_entry; }

  private:

    template <int N> class bitset_functor {
    public:
    bitset_functor(std::bitset<N> &bs) : bits(bs), size(N) { }
      std::bitset<N> &bits;
      int size;
    };

    void next_matching_day(struct tm *next_tm, bool increment);
    void parse_entry(const String &spec, crontab_entry *_entry);
    template <int N> void parse_range(const String &spec, std::bitset<N> &bits,
                                      bool zero_based=true);

    crontab_entry m_entry;
  };

  typedef boost::intrusive_ptr<Crontab> CrontabPtr;

  std::ostream &operator<<(std::ostream &os, const crontab_entry &entry);
  template <int N> void reconstruct_spec(const std::bitset<N> &bits,
                                         String &spec);
}

#endif // HYPERTABLE_CRONTAB_H
