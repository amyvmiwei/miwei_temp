/** -*- C++ -*-
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

#include "Common/Compat.h"
#include "Common/Crontab.h"
#include "Common/Error.h"

#include <iostream>

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace std;

namespace {

  const char *specs[] = {
    "1 1 * *",
    "1 1 * * * *",
    "1-2/2/4 1 * * *",
    "2-1 1 * * *",
    "1-8/ 1 * * *",
    "1-8/a 1 * * *",
    "*-8 1 * * *",
    "1-2b 1 * * *",
    "1-2-3 1 * * *",
    "1 * * * *",
    "2-3 * * * *",
    "* 1,2,7 * * *",
    "* 0-22/2 * * *",
    "* 0-21/3 * * *",
    "* * */2 * *",
    "60 * * * *",
    "* 24 * * *",
    "* * 0 * *",
    "* * 32 * *",
    "* * * 0 *",
    "* * * 13 *",
    "* * * 4-6 *",
    "* * 15-30 * *",
    (const char *)0
  };

  void display_next_event(CrontabPtr &crontab, struct tm *tmval) {
    char output_buf[32];
    time_t t, next;
    t = mktime(tmval);
    next = crontab->next_event(t);
    ctime_r(&next, output_buf);
    String output(output_buf);
    boost::trim_if(output, boost::is_any_of("\n\t "));
    cout << output << "\n";
  }

  void display_next_event(CrontabPtr &crontab, time_t t) {
    char output_buf[32];
    time_t next;
    next = crontab->next_event(t);
    ctime_r(&next, output_buf);
    String output(output_buf);
    boost::trim_if(output, boost::is_any_of("\n\t "));
    cout << output << "\n";
  }


}

int main(int argc, char *argv[]) {
  CrontabPtr crontab;

  for (size_t i=0; specs[i]; i++) {
    try {
      crontab = new Crontab(specs[i]);
      cout << crontab->entry() << "\n";
    }
    catch (Exception &e) {
      cout << e.what() << " (" << specs[i] << ")\n";
    }
  }

  crontab = new Crontab("0 0 * * *");
  display_next_event(crontab, (time_t)1352793827);

  crontab = new Crontab("0 0 29 2 *");
  cout << crontab->entry() << "\n";

  struct tm tmval;

  memset(&tmval, 0, sizeof(tmval));

  tmval.tm_mday = 2;
  tmval.tm_mon = 8;
  tmval.tm_year = 2012 - 1900;
  display_next_event(crontab, &tmval);

  tmval.tm_mday = 1;
  tmval.tm_mon = 2;
  tmval.tm_year = 2016 - 1900;
  display_next_event(crontab, &tmval);

  tmval.tm_mday = 29;
  tmval.tm_mon = 1;
  tmval.tm_year = 2000 - 1900;
  display_next_event(crontab, &tmval);

  crontab = new Crontab("0 0 * * 0");
  cout << crontab->entry() << "\n";

  tmval.tm_mon = 8;
  tmval.tm_year = 2012 - 1900;

  tmval.tm_mday = 3;
  display_next_event(crontab, &tmval);

  tmval.tm_mday = 10;
  display_next_event(crontab, &tmval);

  crontab = new Crontab("0 0 * * *");
  cout << crontab->entry() << "\n";

  tmval.tm_mon = 8;
  tmval.tm_year = 2012 - 1900;

  tmval.tm_min = 27;
  tmval.tm_hour = 3;
  tmval.tm_mday = 2;
  display_next_event(crontab, &tmval);

  tmval.tm_min = 9;
  tmval.tm_hour = 12;
  tmval.tm_mday = 3;
  display_next_event(crontab, &tmval);

  tmval.tm_min = 44;
  tmval.tm_hour = 17;
  tmval.tm_mday = 4;
  display_next_event(crontab, &tmval);

  tmval.tm_min = 0;
  tmval.tm_hour = 12;
  tmval.tm_mday = 5;
  display_next_event(crontab, &tmval);

  crontab = new Crontab("30 12 3,5 * *");
  cout << crontab->entry() << "\n";

  tmval.tm_mon = 8;

  tmval.tm_min = 0;
  tmval.tm_hour = 15;
  tmval.tm_mday = 3;
  display_next_event(crontab, &tmval);

  tmval.tm_min = 0;
  tmval.tm_hour = 12;
  tmval.tm_mday = 3;
  display_next_event(crontab, &tmval);

  tmval.tm_min = 30;
  tmval.tm_hour = 12;
  tmval.tm_mday = 3;
  display_next_event(crontab, &tmval);

  tmval.tm_min = 31;
  tmval.tm_hour = 12;
  tmval.tm_mday = 3;
  display_next_event(crontab, &tmval);

  return 0;
}
