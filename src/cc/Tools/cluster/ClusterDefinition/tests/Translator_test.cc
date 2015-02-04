/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#include "../TranslationContext.h"
#include "../TranslatorRole.h"
#include "../TranslatorTask.h"

#include <Common/Error.h>
#include <Common/Logger.h>

#include <fstream>
#include <iostream>

namespace {
  const char *bad_tasks[] = {
    "# This task is just a comment",
    "# This task is missing the body\n  task:",
    "# This task is missing the end curly\n  task: roles: master {",
    "# This task is missing the roles token\n  task: { }",
    "# This task is missing roles\n  task: foo roles: { }",
    "# This task has an invalid name\n  task: 8foo { }",
    "# This task has an invalid name too\n  task: foo-bar { }",
    "# This task references an undefined role\n  task: foo roles: slave { }",
    "task: foo {\n  ssh: }",
    "task: foo {\n  ssh: bad-option=1 { hostname } }",
    "task: foo {\n  ssh: { hostname }\n  echo 'yes'\n  echo 'maybe'\n  ssh: }",
    "task: foo {\n  ssh { hostname }\n  echo 'yes'\n  echo 'maybe'\n }",
    "task: foo {\n  echo 'one'\n  ssh in-series { hostname }\n }",
    "task: foo {\n  echo 'one'\n  echo 'two'\n  ssh random-start-delay=5000 { hostname }\n }",
    "task: foo {\n  echo 'before'\n  ssh: { hostname }\n  echo 'yes'\n  echo 'maybe'\n  ssh: }",
    "# Conflict with builtin\n  task: CLUSTER_BUILTIN_display_line { }",
    "# Conflict with builtin\n  task: show_variables { }",
    (const char *)0
  };
  const char *bad_roles[] = {
    "role: foo ht-good-master\n",
    "role: foo ht-good - master\n",
    "role: foo ht-good -master\n",
    "role: foo ht-good -slave\n",
    nullptr
  };
}

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace std;

int main(int argc, char **argv) {
  TranslationContext context;

  if (argc != 2) {
    cout << "usage: Translator_test <golden-file>" << endl;
    exit(1);
  }

  ofstream output_file;
  output_file.open("Translator_test.output");

  for (size_t i=0; bad_tasks[i]; i++) {
    try {
      TranslatorTask task("test.def", 8, bad_tasks[i]);
      string translated = task.translate(context);
      HT_FATALF("Bad task %d test failure", (int)i);
    }
    catch (Exception &e) {
      output_file << Error::get_text(e.code()) << " - " << e.what() << endl;
    }
  }

  context.roles.insert("master");
  for (size_t i=0; bad_roles[i]; i++) {
    try {
      TranslatorRole role("test.def", 8, bad_roles[i]);
      string translated = role.translate(context);
      output_file << "ok: " << translated << endl;
    }
    catch (Exception &e) {
      output_file << Error::get_text(e.code()) << " - " << e.what() << endl;
    }
  }
    
  output_file.close();

  string cmd = format("diff Translator_test.output %s", argv[1]);
  if (system(cmd.c_str()))
    exit(1);

  return 0;
}
