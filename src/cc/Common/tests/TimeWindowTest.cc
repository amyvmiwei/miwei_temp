/* -*- C++ -*-
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include <Common/Compat.h>
#include <Common/Error.h>
#include <Common/TimeWindow.h>

#include <boost/algorithm/string.hpp>

#include <ctime>
#include <iostream>
#include <vector>

using namespace Hypertable;
using namespace std;

int main(int argc, char *argv[]) {
  vector<String> specs;

  specs.push_back("* 2-5 * * *");
  specs.push_back("* 14-17 * * *");

  TimeWindow low_activity(specs);

  struct tm tmval;
  memset(&tmval, 0, sizeof(tmval));
  tmval.tm_mday = 25;
  tmval.tm_mon = 11;
  tmval.tm_year = 2013 - 1900;
  time_t start_time = mktime(&tmval)+1;
  

  for (time_t t=start_time; t<=start_time+86400; t+=60) {
    low_activity.update_current_time(t);
    if (low_activity.within_window())
      cout << ctime(&t);
  }

  return 0;
}
