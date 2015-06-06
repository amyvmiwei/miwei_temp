/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#include <Common/Compat.h>

#include "CommTestThreadFunction.h"

#include <AsyncComm/Comm.h>
#include <AsyncComm/ConnectionManager.h>
#include <AsyncComm/Event.h>
#include <AsyncComm/ReactorFactory.h>

#include <Common/Init.h>
#include <Common/Error.h>
#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/TestHarness.h>
#include <Common/System.h>
#include <Common/Usage.h>

#include <boost/thread/thread.hpp>

#include <chrono>
#include <cstdlib>
#include <thread>

extern "C" {
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
}

using namespace std;
using namespace Hypertable;

namespace {
  const char *usage[] = {
    "usage: commTestTimer",
    "",
    "This program tests tests timer logic of AsyncComm.",
    0
  };

  /**
   *
   */
  class TimerHandler : public DispatchHandler {
  public:
    TimerHandler(const char *msg)
      : m_msg(msg) { return; }
    virtual void handle(EventPtr &event_ptr) {
      HT_INFOF("%s (%s)", event_ptr->to_str().c_str(), m_msg);
    }
  private:
    const char *m_msg;
  };

  struct TimerRec {
    uint32_t delay;
    const char *msg;
  };

  struct TimerRec history[] = {
    { 1, "Local" },
    { 2, "media" },
    { 3, "have" },
    { 4, "reported" },
    { 5, "eyewitness" },
    { 6, "accounts" },
    { 7, "of" },
    { 8, "a" },
    { 9, "fiery" },
    { 10, "ball" },
    { 35, 0 }
  };
}


int main(int argc, char **argv) {
  Comm *comm;
  int error;
  TestHarness harness("commTestTimer");
  bool golden = false;
  uint32_t wait_time = 0;

  Config::init(argc, argv);

  if (argc > 1) {
    if (!strcmp(argv[1], "--golden"))
      golden = true;
    else
      Usage::dump_and_exit(usage);
  }

  srand(8876);

  ReactorFactory::initialize(1);

  comm = Comm::instance();

  for (int i=0; history[i].msg; i++) {
    DispatchHandlerPtr handler = std::make_shared<TimerHandler>(history[i].msg);
    if ((error = comm->set_timer(history[i].delay*1000, handler))
        != Error::OK) {
      HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
      exit(EXIT_FAILURE);
    }
    if (history[i].delay > wait_time)
      wait_time = history[i].delay;
  }

  this_thread::sleep_for(chrono::milliseconds((wait_time+1)*1000));

  if (!golden)
    harness.validate_and_exit("commTestTimer.golden");

  harness.regenerate_golden_file("commTestTimer.golden");

  quick_exit(EXIT_SUCCESS);
}
