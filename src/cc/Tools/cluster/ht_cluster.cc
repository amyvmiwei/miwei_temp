/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#include <Common/Compat.h>

#include "ClusterCommandInterpreter.h"
#include "ClusterDefinition/Compiler.h"

#include <Tools/Lib/CommandShell.h>

#include <Hypertable/Lib/Config.h>

#include <Common/Config.h>
#include <Common/FileUtils.h>
#include <Common/Init.h>
#include <Common/System.h>

#include <cerrno>
#include <iostream>

extern "C" {
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
}

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace Hypertable::Config;

/// @defgroup cluster cluster
/// @ingroup Tools
/// Cluster task automation tool.

namespace {

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc().add_options()
        ("definition,f", str(), "Definition file name")
        ;
    }
  };

  const string locate_definition_file() {
    string fname;

    if (has("definition")) {
      fname = get_str("definition");
      if (!FileUtils::exists(fname)) {
        cout << "Definition file '" << fname << "' does not exist." << endl;
        exit(1);
      }
      return fname;
    }
    
    // conf dir
    fname = System::install_dir + "/conf/cluster.def";
    if (FileUtils::exists(fname))
      return fname;

    // CWD
    fname.clear();
    char cwd[1024];
    if (getcwd(cwd, 1024) == nullptr) {
      cout << "getcwd() - " << strerror(errno) << endl;
      exit(1);
    }
    fname.append(cwd);
    fname.append("/cluster.def");
    if (FileUtils::exists(fname))
      return fname;

    // HOME directory
    fname.clear();
    struct passwd *pw = getpwuid(getuid());
    fname.append(pw->pw_dir);
    fname.append("/cluster.def");
    if (FileUtils::exists(fname))
      return fname;

    cout << "Unable to locate 'cluster.def' in '" << System::install_dir 
         << "/conf', " << "'.'" << ", or '" << pw->pw_dir << "'" << endl;
    exit(1);
    
  }

}

typedef Meta::list<AppPolicy, CommandShellPolicy, DefaultPolicy> Policies;


int main(int argc, char **argv) {

  CommandShellPtr shell;
  CommandInterpreterPtr interp;
  int status = 0;

  try {
    init_with_policies<Policies>(argc, argv);

    Compiler compiler(locate_definition_file());

    exit(0);

    interp = new ClusterCommandInterpreter();
    shell = new CommandShell("cluster", interp, properties);

    // Entire line is command
    shell->set_line_command_mode(true);

    interp->set_silent(shell->silent());
    interp->set_test_mode(shell->test_mode());

    status = shell->run();
  }
  catch(Exception &e) {
    cout << Error::get_text(e.code()) << " - " << e.what() << endl;
    status = e.code();
  }
  _exit(status);
}
