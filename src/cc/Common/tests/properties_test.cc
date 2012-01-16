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
#include "Common/Logger.h"
#include "Common/Properties.h"
#include <iostream>

using namespace Hypertable;
using namespace Property;
using namespace std;

namespace {

enum Mode { MODE_FOO, MODE_BAR };

void basic_test(const PropertiesDesc &desc) {
  static const char *argv[] = { "test", "--string", "foo", "--strs", "s1",
      "--i16", "64k", "--i32", "20M", "--i64", "8g", "--strs", "s2",
      "--int64s", "16G", "--int64s", "32G" };
  vector<String> args;
  args.push_back("--f64"); args.push_back("1k");
  args.push_back("--float64s"); args.push_back("1.5");
  args.push_back("--float64s"); args.push_back("1.5K");
  args.push_back("--float64s"); args.push_back("1.5M");
  args.push_back("--float64s"); args.push_back("1.5g");

  Properties props;
  props.parse_args(sizeof(argv)/sizeof(char *), (char **)argv, desc);
  props.parse_args(args, desc);
  props.notify();
  props.set("mode", MODE_FOO);
  cout <<"mode="<< props.get<Mode>("mode") << endl;
  props.print(cout);
}

void notify(const String &s) {
  cout << __func__ <<": "<< s << endl;
}

}

int main(int argc, char *argv[]) {
  try {
    double f64test = 0;
    PropertiesDesc desc;
    desc.add_options()
      ("help,h", "help")
      ("boo", boo()->zero_tokens()->default_value(true), "a boolean arg")
      ("string", str()->notifier(notify), "a string arg")
      ("strs", strs(), "a list of strings")
      ("i16", i16(), "a 16-bit integer arg")
      ("i32", i32(), "a 32-bit integer arg")
      ("i64", i64(), "a 64-bit integer arg")
      ("int64s", i64s(), "a list of 64-bit integers")
      ("f64", f64(&f64test), "a double arg")
      ("float64s", f64s(), "a list of doubles")
      ;
    Properties props;
    props.parse_args(argc, argv, desc);
    props.notify();

    if (props.has("help"))
      cout << desc << endl;

    cout <<"f64test="<< f64test << endl;

    props.print(cout, true);
    HT_TRY("basic test", basic_test(desc));
  }
  catch (Exception &e) {
    cout << e << endl;
    return 1;
  }
  return 0;
}
