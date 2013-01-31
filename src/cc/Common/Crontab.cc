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
#include "Compat.h"
#include "Crontab.h"
#include "Logger.h"
#include "StringExt.h"
#include "Sweetener.h"

#include <cctype>
#include <cstdlib>

#include <boost/tokenizer.hpp>

using namespace Hypertable;
using namespace boost;
using namespace std;

Crontab::Crontab(const String &spec) {
  parse_entry(spec, &m_entry);
}

time_t Crontab::next_event(time_t now) {
  struct tm next_tm;
  int i;
  int hour_increment = 0;
  int day_increment = 0;

  localtime_r(&now, &next_tm);
  next_tm.tm_sec = 0;

 next_day:
  next_matching_day(&next_tm, day_increment);

 next_hour:
  for (i=next_tm.tm_hour+hour_increment; i<24; i++) {
    if (m_entry.hour[i]) {
      if (i > next_tm.tm_hour) {
        next_tm.tm_hour = i;
        next_tm.tm_min = 0;
      }
      break;
    }
  }
  if (i == 24) {
    day_increment = 1;
    hour_increment = 0;
    goto next_day;
  }

  for (i=next_tm.tm_min; i<60; i++) {
    if (m_entry.minute[i]) {
      next_tm.tm_min = i;
      return mktime(&next_tm);
    }
  }
  if (i == 60) {
    hour_increment = 1;
    next_tm.tm_min = 0;
    goto next_hour;
  }

  return mktime(&next_tm);
}

void Crontab::next_matching_day(struct tm *next_tm, bool increment) {
  time_t t;
  bool advanced = false;

  if (increment) {
    next_tm->tm_sec = 0;
    next_tm->tm_min = 0;
    next_tm->tm_hour = 0;
    t = mktime(next_tm) + 86400;
    localtime_r(&t, next_tm);
    advanced = true;
  }

  while (true) {
    if (m_entry.month[next_tm->tm_mon] &&
        (m_entry.dom[(next_tm->tm_mday-1)] || m_entry.dow[next_tm->tm_wday]))
      break;
    t = mktime(next_tm) + 86400;
    localtime_r(&t, next_tm);
    advanced = true;
  }
  if (advanced) {
    next_tm->tm_sec = 0;
    next_tm->tm_min = 0;
    next_tm->tm_hour = 0;
  }
}


void Crontab::parse_entry(const String &spec, crontab_entry *entry) {
  String text = spec;
  String fields[5];
  size_t field_count = 0;
  bool wildcard_dom = false;
  bool wildcard_dow = false;

  {
    char_separator<char> sep(" ");
    tokenizer< char_separator<char> > tokens(text, sep);
    foreach_ht (const string& t, tokens) {
      if (field_count == 5)
        HT_THROW(Error::COMMAND_PARSE_ERROR, "too many fields");
      fields[field_count++] = t;
    }
    if (field_count < 5)
      HT_THROW(Error::COMMAND_PARSE_ERROR, "too few fields");
  }

  // minute
  {
    char_separator<char> sep(",");
    tokenizer< char_separator<char> > tokens(fields[0], sep);
    foreach_ht (const string& t, tokens)
      parse_range<60>(t, entry->minute);
  }

  // hour
  {
    char_separator<char> sep(",");
    tokenizer< char_separator<char> > tokens(fields[1], sep);
    foreach_ht (const string& t, tokens)
      parse_range<24>(t, entry->hour);
  }

  // dom
  {
    if (fields[2] == "*")
      wildcard_dom = true;
    else {
      char_separator<char> sep(",");
      tokenizer< char_separator<char> > tokens(fields[2], sep);
      foreach_ht (const string& t, tokens)
        parse_range<31>(t, entry->dom, false);
    }
  }

  // month
  {
    char_separator<char> sep(",");
    tokenizer< char_separator<char> > tokens(fields[3], sep);
    foreach_ht (const string& t, tokens)
      parse_range<12>(t, entry->month, false);
  }

  // dow
  {
    if (fields[4] == "*")
      wildcard_dow = true;
    else {
      char_separator<char> sep(",");
      tokenizer< char_separator<char> > tokens(fields[4], sep);
      foreach_ht (const string& t, tokens)
        parse_range<8>(t, entry->dow);
      if (entry->dow[7])
        entry->dow[0] = true;
      if (entry->dow[0])
        entry->dow[7] = true;
    }
  }

  if (wildcard_dom) {
    if (wildcard_dow)
      entry->dom.set();
    else
      entry->dom.reset();
  }
  else {
    if (wildcard_dow)
      entry->dow.reset();
  }
  
}

template <int N> 
void Crontab::parse_range(const String &spec, std::bitset<N> &bits, bool zero_based) {
  String text = spec;
  String range, step_str;
  size_t count = 0;

  // Split into range & step
  int step = -1;
  {
    char_separator<char> sep("/");
    tokenizer< char_separator<char> > tokens(text, sep);
    foreach_ht (const string& t, tokens) {
      if (count == 0)
        range = t;
      else if (count == 1)
        step_str = t;
      else
        HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");
      count++;
    }

    if (count == 2) {
      if (step_str == "")
        HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");
      for (const char *ptr = step_str.c_str(); *ptr; ptr++) {
        if (!isdigit(*ptr))
          HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");
      }
      step = atoi(step_str.c_str());
    }
  }

  count = 0;
  int start = -1;
  int end = -1;
  bool got_wildcard = false;
  {
    char_separator<char> sep("-");
    tokenizer< char_separator<char> > tokens(range, sep);
    foreach_ht (const string& t, tokens) {
      if (count == 0) {
        if (t == "*") {
          bits.set();
          got_wildcard = true;
        }
        else {
          for (const char *ptr = t.c_str(); *ptr; ptr++) {
            if (!isdigit(*ptr))
              HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");
          }
          start = atoi(t.c_str());
        }
      }
      else if (count == 1) {
        if (got_wildcard)
          HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");
        for (const char *ptr = t.c_str(); *ptr; ptr++) {
          if (!isdigit(*ptr))
            HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");
        }
        end = atoi(t.c_str());
      }
      else
        HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");
      count++;
    }
  }

  if (got_wildcard)
    return;

  int offset = 0;
  if (!zero_based) {
    offset = 1;
    if (start == 0)
      HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");
  }

  if (start == -1)
    HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");

  if (start >= (N+offset) || end >= (N+offset))
    HT_THROW(Error::COMMAND_PARSE_ERROR, "bad specification");


  if (end == -1)
    bits[start-offset] = true;

  if (step == -1)
    step = 1;

  for (int i=(start-offset); i<=(end-offset); i+=step)
    bits[i] = true;
}

std::ostream &Hypertable::operator<<(std::ostream &os,
                                     const Hypertable::crontab_entry &entry) {
  String spec;

  reconstruct_spec<60>(entry.minute, spec);
  os << spec;
  reconstruct_spec<24>(entry.hour, spec);
  os << " " << spec;
  reconstruct_spec<31>(entry.dom, spec);
  os << " " << spec;
  reconstruct_spec<12>(entry.month, spec);
  os << " " << spec;
  reconstruct_spec<8>(entry.dow, spec);
  os << " " << spec;

  return os;
}

template <int N> void Hypertable::reconstruct_spec(const std::bitset<N> &bits,
                                                   String &spec) {
  bool first = true;
  int start = -1;
  int end = -1;
  spec = "";
  for (int i=0; i<N; i++) {
    if (bits[i]) {
      if (start == -1)
        start = i;
      else
        end = i;
    }
    else if (start != -1) {
      if (!first)
        spec += ",";
      first = false;
      if (i == start+1)
        spec += String("") + start;
      else
        spec += String("") + start + "-" + end;
      start = end = -1;
    }
  }
  if (start != -1) {
    if (!first)
      spec += ",";
    if (start == 0 && end == N-1)
      spec += "*";
    else {
      if (end == -1)
        spec += String("") + start;
      else
        spec += String("") + start + "-" + end;
    }
  }
  else if (first)
    spec += "*";
}
