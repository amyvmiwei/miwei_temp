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
#include "ClusterDefinition/TokenizerTools.h"

#include <Tools/Lib/CommandShell.h>

#include <Hypertable/Lib/Config.h>

#include <Common/Config.h>
#include <Common/FileUtils.h>
#include <Common/Init.h>
#include <Common/System.h>
#include <Common/Version.h>

#include <cerrno>
#include <iostream>
#include <string>
#include <vector>

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

  void clear_cache() {
    string cache_dir;
    struct passwd *pw = getpwuid(getuid());
    cache_dir.append(pw->pw_dir);
    cache_dir.append("/.cluster");
    if (!FileUtils::exists(cache_dir))
      return;
    string cmd = (string)"/bin/rm -rf " + cache_dir;
    if (system(cmd.c_str()) != 0) {
      cout << "Failed execution: " << cmd << endl;
      _exit(1);
    }
  }

  const string locate_definition_file() {
    string fname;

    // CWD
    fname.clear();
    char cwd[1024];
    if (getcwd(cwd, 1024) == nullptr) {
      cout << "getcwd() - " << strerror(errno) << endl;
      _exit(1);
    }
    fname.append(cwd);
    fname.append("/cluster.def");
    if (FileUtils::exists(fname))
      return fname;

    // conf dir
    fname = System::install_dir + "/conf/cluster.def";
    if (FileUtils::exists(fname))
      return fname;

    cout << "Unable to locate 'cluster.def' in '.' or '" << System::install_dir 
         << "/conf'" << endl;
    _exit(1);
  }

  bool is_environment_setting(const string &arg) {
    HT_ASSERT(!arg.empty());
    if (!TokenizerTools::is_identifier_start_character(arg[0]))
      return false;
    const char *ptr = arg.c_str();
    while (TokenizerTools::is_identifier_character(*ptr))
      ptr++;
    return *ptr == '=';
  }

  void exec_command(const string &script, vector<string> environment,
                    vector<string> arguments) {
    char **argv = new char *[environment.size() + arguments.size() + 3];

    if (environment.empty()) {
      argv[0] = (char *)script.c_str();
      size_t i=1;
      for (auto & arg : arguments)
        argv[i++] = (char *)arg.c_str();
      argv[i] = nullptr;
      if (execv(script.c_str(), argv) < 0) {
        cout << "execv() failed - " << strerror(errno) << endl;
        _exit(1);
      }
    }
    else {
      argv[0] = (char *)"env";
      size_t i=1;
      for (auto & setting : environment)
        argv[i++] = (char *)setting.c_str();
      argv[i++] = (char *)script.c_str();
      for (auto & arg : arguments)
        argv[i++] = (char *)arg.c_str();
      argv[i] = nullptr;
      if (execvp("env", argv) < 0) {
        cout << "execv() failed - " << strerror(errno) << endl;
        _exit(1);
      }
    }
    HT_FATAL("Should not reach here!");
  }

  const char *usage[] = {
    "",
    "usage: ht_cluster [<options>] [<environment>] <task> [<arg> ...]",
    "usage: ht_cluster",
    "",
    "options:",
    "  --clear-cache     Clear command script cache directory (~/.cluster)",
    "  --display-script  Display the command script compiled from the cluster",
    "                    definition file",
    "  -f <fname>        Use <fname> for the definition file",
    "  --help            Display this help message",
    "  --version         Display version string",
    "",
    "This program is used to run tasks specified in a cluster definition file.",
    "The default name of the cluster definition file is \"cluster.def\" and is",
    "first searched for in the current working directory and if not found, then",
    "it is searched for in the directory ../conf/ relative to the ht_cluster",
    "executable file.  The cluster definition file can also be explicitly",
    "specified with the -f option.  This program can be run in two modes,",
    "command-line mode or interactive mode.",
    "",
    "Command-line mode",
    "-----------------",
    "In this mode, the task name and its arguments are passed in on the",
    "command-line.  Optionally, environment variables can be set by supplying a",
    "list of space-separated <name>=<value> variable specifications where <name>",
    "is the name of the environment variable to set and <value> is the value you",
    "would like to set it to prior to running the task.  The environment",
    "variables must appear before the task name on the command line.",
    "",
    "Interactive mode",
    "----------------",
    "ht_cluster will enter interactive mode if it is run without any arguments.",
    "In this mode tasks or any shell command can be run interactively.  Tasks",
    "can be run on their default roles or any set of machines matching a host",
    "specification.  Shell commands can be run on any set of roles or on any",
    "set of machines matching a host specification.  Type 'help' at the",
    "interactive command prompt for details on the format of interactive",
    "commands.",
    "",
    nullptr
  };

}


int main(int argc, char **argv) {

  CommandShellPtr shell;
  CommandInterpreterPtr interp;
  int status = 0;

  try {
    vector<string> environment;
    string definition_file;
    vector<string> arguments;
    bool display_script {};

    System::initialize();
    Config::properties = new Properties();
    ReactorFactory::initialize(System::get_processor_count());

    // environment settings and cluster options
    int i = 1;
    while (i<argc) {
      if (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-h")) {
        for (size_t i=0; usage[i]; i++)
          cout << usage[i] << "\n";
        cout << flush;
        _exit(0);
      }
      else if (!strcmp(argv[1], "--version")) {
        cout << version_string() << endl;
        _exit(0);
      }
      else if (!strcmp(argv[1], "--clear-cache")) {
        clear_cache();
        _exit(0);
      }
      else if (!strcmp(argv[i], "-f")) {
        if (!definition_file.empty()) {
          cout << "error: -f option supplied multiple times" << endl;
          _exit(1);
        }
        i++;
        if (i == argc) {
          cout << "error: missing argument to -f option" << endl;
          _exit(1);
        }
        definition_file.append(argv[i]);
        i++;
        continue;
      }
      else if (!strcmp(argv[i], "--display-script")) {
        display_script = true;
      }
      else if (!is_environment_setting(argv[i]))
        break;
      environment.push_back(argv[i]);
      i++;
    }

    // load command arguments
    while (i<argc) {
      arguments.push_back(argv[i]);
      i++;
    }

    if (definition_file.empty())
      definition_file = locate_definition_file();

    if (display_script) {
      Compiler compiler(definition_file);
      string cmd = (string)"cat " + compiler.output_script();
      if (system(cmd.c_str()) != 0) {
        cout << "Failed execution: " << cmd << endl;
        _exit(1);
      }
      _exit(0);
    }

    Compiler compiler(definition_file);

    if (!arguments.empty())
      exec_command(compiler.output_script(), environment, arguments);

    interp = new ClusterCommandInterpreter(compiler.output_script());
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
