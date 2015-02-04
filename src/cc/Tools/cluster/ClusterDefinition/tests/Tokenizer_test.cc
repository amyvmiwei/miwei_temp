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

#include <Tools/cluster/ClusterDefinition/Tokenizer.h>

#include <Common/Error.h>
#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/String.h>

#include <cerrno>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <string>

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace std;

namespace {
  
  const char *bad_input[] = {
    "# comment\n\nrole master test00\n",
    "# comment\n\nrole: master test00\ntask doit { echo \"done\" }",
    "# comment\n\nrole: master test00\ninclude cluster.tasks\ntask: doit { echo \"done\" }",
    nullptr
  };

}

int main(int argc, char **argv) {
  string line;
  string content;

  if (argc != 3) {
    cout << "usage: Tokenizer_test <input-file> <golden-file>" << endl;
    exit(1);
  }

  ifstream input_file(argv[1]);

  if (!input_file.is_open()) {
    cout << "Unable to open input file '" << argv[1] << "'" << endl;
    exit(1);
  }

  ofstream output_file;
  output_file.open("Tokenizer_test.output");

  while (getline(input_file, line)) {

    if (!strncmp(line.c_str(), "#### test-definition:", 21)) {
      if (!content.empty()) {
        Tokenizer tokenizer(argv[1], content);
        Token token;
        while (tokenizer.next(token)) {
          output_file << "Token " << Token::type_to_text(token.type) << endl;
          output_file << token.text;
        }
      }
      content.clear();
    }
    content.append(line);
    content.append("\n");
  }

  if (!content.empty()) {
    Tokenizer tokenizer(argv[1], content);
    Token token;
    while (tokenizer.next(token)) {
      output_file << "Token " << Token::type_to_text(token.type) << endl;
      output_file << token.text;
    }
  }

  input_file.close();

  size_t i=0;
  while (bad_input[i]) {
    try {
      Tokenizer tokenizer(argv[1], bad_input[i]);
      Token token;
      while (tokenizer.next(token))
        ;
    }
    catch (Exception &e) {
      output_file << Error::get_text(e.code()) << " - " << e.what() << endl;
    }
    i++;
  }

  output_file.close();

  string cmd = format("diff Tokenizer_test.output %s", argv[2]);
  if (system(cmd.c_str()))
    exit(1);

  return 0;
}
