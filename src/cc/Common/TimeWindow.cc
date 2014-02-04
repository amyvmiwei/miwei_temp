/*
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
/// Definitions for TimeWindow.
/// This file contains definitions for TimeWindow, a class for defining a
/// set of time windows and checking if the current time falls with any of
/// those windows.

#include <Common/Compat.h>
#include "TimeWindow.h"

#include <Common/Crontab.h>
#include <Common/Error.h>
#include <Common/Logger.h>

#include <boost/algorithm/string.hpp>

#include <ctime>

using namespace Hypertable;

TimeWindow::TimeWindow(std::vector<String> crontab_specs) 
  : m_within_window(false), m_enabled(true) {

  foreach_ht (String &spec, crontab_specs) {
    try {
      boost::trim_if(spec, boost::is_any_of("\"'"));
      if (!spec.empty())
        m_crontabs.push_back( Crontab(spec) );
    }
    catch (Exception &e) {
      HT_FATALF("Problem parsing time window crontab entry (%s) - %s",
                spec.c_str(), e.what());
    }

  }
}

bool TimeWindow::update_current_time(time_t now) {
  bool window_status_on_entry = m_within_window;
  time_t next;

  if (now == 0)
    now = time(0);

  foreach_ht (Crontab &crontab, m_crontabs) {
    next = crontab.next_event(now);
    HT_ASSERT(next >= now);
    // if next event occurs within the next minute, we're within window
    if (next - now <= (time_t)60) {
      m_within_window = true;
      return window_status_on_entry != m_within_window;
    }
  }
  m_within_window = false;
  return window_status_on_entry != m_within_window;
}
