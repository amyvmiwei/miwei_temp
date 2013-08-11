/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#include "Common/Compat.h"
#include "Common/Logger.h"
#include "Common/Init.h"

#include "AsyncComm/Config.h"

#include "Hypertable/Master/SystemState.h"

extern "C" {
#include <poll.h>
}

#include <fstream>
#include <iostream>
#include <vector>

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

  typedef Meta::list<GenericServerPolicy, DefaultCommPolicy> Policies;

} // local namespace


int main(int argc, char **argv) {

  try {
    init_with_policies<Policies>(argc, argv);
    ofstream out("system_state_test.output");

    properties->set("Hypertable.Master.NotificationInterval", 1);

    SystemState system_state;
    vector<NotificationMessage> notifications;

    /*
     * Single code ADMIN set API
     */

    if (!system_state.admin_set(SystemVariable::READONLY, true))
      HT_FATAL("admin_set(SystemVariable::READONLY, true) did not change state");

    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after admin_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    poll(0, 0, 2000);

    if (system_state.admin_set(SystemVariable::READONLY, true))
      HT_FATAL("admin_set(SystemVariable::READONLY, true) changed state");
    
    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after admin_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    poll(0, 0, 2000);

    if (system_state.admin_set(SystemVariable::READONLY, true))
      HT_FATAL("admin_set(SystemVariable::READONLY, true) changed state");
    
    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after admin_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    if (!system_state.admin_set(SystemVariable::READONLY, false))
      HT_FATAL("admin_set(SystemVariable::READONLY, false) did not change state");

    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after admin_set(SystemVariable::READONLY, false)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    if (system_state.admin_set(SystemVariable::READONLY, false))
      HT_FATAL("admin_set(SystemVariable::READONLY, false) changed state");

    if (system_state.get_notifications(notifications))
      HT_FATAL("Notification generated after admin_set(SystemVariable::READONLY, false)");

    /*
     * Vector of codes ADMIN set API
     */

    vector<SystemVariable::Spec> specs;
    SystemVariable::Spec spec;

    spec.code = SystemVariable::READONLY;
    spec.value = true;
    specs.push_back(spec);

    if (!system_state.admin_set(specs))
      HT_FATAL("admin_set(SystemVariable::READONLY, true) did not change state");

    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after admin_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    poll(0, 0, 2000);

    specs.clear();
    spec.code = SystemVariable::READONLY;
    spec.value = true;
    specs.push_back(spec);

    if (system_state.admin_set(specs))
      HT_FATAL("admin_set(SystemVariable::READONLY, true) changed state");
    
    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after admin_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    poll(0, 0, 2000);

    specs.clear();
    spec.code = SystemVariable::READONLY;
    spec.value = true;
    specs.push_back(spec);

    if (system_state.admin_set(specs))
      HT_FATAL("admin_set(SystemVariable::READONLY, true) changed state");
    
    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after admin_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    specs.clear();
    spec.code = SystemVariable::READONLY;
    spec.value = false;
    specs.push_back(spec);

    if (!system_state.admin_set(specs))
      HT_FATAL("admin_set(SystemVariable::READONLY, false) did not change state");

    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after admin_set(SystemVariable::READONLY, false)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    specs.clear();
    spec.code = SystemVariable::READONLY;
    spec.value = false;
    specs.push_back(spec);

    if (system_state.admin_set(specs))
      HT_FATAL("admin_set(SystemVariable::READONLY, false) changed state");

    if (system_state.get_notifications(notifications))
      HT_FATAL("Notification generated after admin_set(SystemVariable::READONLY, false)");

    /*
     * Single code AUTO set API
     */

    if (!system_state.auto_set(SystemVariable::READONLY, true, "because of test (1)"))
      HT_FATAL("auto_set(SystemVariable::READONLY, true) did not change state");

    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after auto_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    poll(0, 0, 2000);

    if (system_state.auto_set(SystemVariable::READONLY, true, "because of test (2)"))
      HT_FATAL("auto_set(SystemVariable::READONLY, true) changed state");
    
    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after auto_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    poll(0, 0, 2000);

    if (system_state.auto_set(SystemVariable::READONLY, true, "because of test (3)"))
      HT_FATAL("auto_set(SystemVariable::READONLY, true) changed state");
    
    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after auto_set(SystemVariable::READONLY, true)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    if (!system_state.auto_set(SystemVariable::READONLY, false, "because of test (4)"))
      HT_FATAL("auto_set(SystemVariable::READONLY, false) did not change state");

    if (!system_state.get_notifications(notifications))
      HT_FATAL("No notification generated after auto_set(SystemVariable::READONLY, false)");

    if (notifications.size() != 1)
      HT_FATALF("Got %d notifications, expected 1", (int)notifications.size());

    out << notifications[0].subject << "\n";
    out << notifications[0].body << "\n";

    if (system_state.auto_set(SystemVariable::READONLY, false, "because of test (5)"))
      HT_FATAL("auto_set(SystemVariable::READONLY, false) changed state");

    if (system_state.get_notifications(notifications))
      HT_FATAL("Notification generated after auto_set(SystemVariable::READONLY, false)");

    out.close();

    String cmd = "diff system_state_test.output system_state_test.golden";
    if (system(cmd.c_str()) != 0) {
      std::cout << "system_state_test.output differs from golden file.\n";
      _exit(1);
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }
  return 0;
}
