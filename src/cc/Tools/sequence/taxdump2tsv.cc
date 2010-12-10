/**
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License.
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

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <map>

extern "C" {
#include <time.h>
#include <sys/time.h>
}

#include "AsyncComm/Config.h"
#include "Common/DynamicBuffer.h"
#include "Common/FlyweightString.h"
#include "Common/Init.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace {

  const char *usage =
    "\nusage: prune_tsv [options] <past-date-offset>\n\n"
    "description:\n"
    "  This program converts a Genes table load file (.tsv) to an Ngrams table\n"
    "  load file\n"
    "options";

  struct AppPolicy : Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("reverse-map", boo()->zero_tokens()->default_value(false),
         "Generate reverse mapping (tax_id -> full_key)")
        ;
    }
  };

  typedef Meta::list<AppPolicy, DefaultCommPolicy> Policies;

  inline void get_fields(char *line, std::vector<const char *> &fields) {
    char *ptr, *base = line;

    fields.clear();

    ptr = strtok(base, "|");
    while (ptr) {
      base = ptr;
      while (*base && (*base == ' ' || *base == '\t'))
        base++;
      if (*base) {
        for (ptr = base + strlen(base)-1; ptr>base && (*(ptr-1)==' '||*(ptr-1)=='\t'); ptr--)
          ;
        *ptr=0;
      }
      if (*base)
        fields.push_back(base);
      else
        fields.push_back(0);
      ptr = strtok(0, "|");
    }
  }

  struct node {
    int tax_id;
    int parent_id;
    const char *rank;
    const char *embl_code;
    std::vector<struct node *> children;
  };

  struct LtNode {
    bool operator()(struct node n1, struct node n2) const {
      if (n1.parent_id == n2.parent_id)
        return n1.tax_id < n2.tax_id;
      return n1.parent_id < n2.parent_id;
    }
  };

  std::vector<String> path_stack;

  bool reverse_map = false;

  void dfs(struct node *n) {
    char buf[32];
    String path;
    sprintf(buf, "%d", n->tax_id);
    path_stack.push_back(String(buf));

    for (size_t i=0; i<path_stack.size(); i++)
      path += String("/") + path_stack[i];
    if (reverse_map) {
      cout << n->tax_id << "\tfull_key\t" << path << "\n";
    }
    else {
      cout << path << "\ttax_id\t" << n->tax_id << "\n";
      if (n->rank)
        cout << path << "\trank\t" << n->rank << "\n";
      if (n->embl_code)
        cout << path << "\tembl_code\t" << n->embl_code << "\n";
    }

    for (size_t i=0; i<n->children.size(); i++) {
      if (n != n->children[i])
        dfs(n->children[i]);
    }
    path_stack.resize(path_stack.size()-1);
  }

  
}



/**
 *
 */
int main(int argc, char **argv) {
  uint64_t lineno = 0;
  std::vector<const char *> fields;
  FlyweightString fws;
  std::vector<struct node> nodes;
  struct node tmp_node;

  char *line_buffer = new char [ 50 * 1024 * 1024 ];

  ios::sync_with_stdio(false);

  try {
    init_with_policies<Policies>(argc, argv);

    reverse_map = get_bool("reverse-map");

    while (!cin.eof()) {

      if (cin.fail()) {
	cerr << "istream failure (line " << lineno << ")" << endl;
	cin.clear();
      }

      cin.getline(line_buffer, 50*1024*1024);
      lineno++;

      if (*line_buffer == '#')
	continue;

      get_fields(line_buffer, fields);

      tmp_node.tax_id = atoi(fields[0]);
      tmp_node.parent_id = atoi(fields[1]);
      tmp_node.rank = fws.get(fields[2]);
      tmp_node.embl_code = fws.get(fields[3]);

      nodes.push_back(tmp_node);
    }

    struct LtNode lt;
    sort(nodes.begin(), nodes.end(), lt);

    hash_map<int, struct node *> node_map;
    for (size_t i=0; i<nodes.size(); i++)
      node_map[nodes[i].tax_id] = &nodes[i];

    if (nodes[0].tax_id != 1 || nodes[0].parent_id != 1) {
      cerr << "Root node not found" << endl;
      exit(1);
    }

    int last_id = 1;
    size_t start=0;
    for (size_t i=1; i<nodes.size(); i++) {
      if (nodes[i].parent_id != last_id) {
        hash_map<int, struct node *>::iterator iter = node_map.find(last_id);
        if (iter == node_map.end()) {
          cerr << "Can't find entry for node " << last_id << endl;
          exit(1);
        }
        (*iter).second->children.reserve(i-start);
        for (size_t j=start; j<i; j++)
          (*iter).second->children.push_back(&nodes[j]);
        last_id = nodes[i].parent_id;
        start = i;
      }
    }

    hash_map<int, struct node *>::iterator iter = node_map.find(last_id);
    if (iter == node_map.end()) {
      cerr << "Can't find entry for node " << last_id << endl;
      exit(1);
    }
    (*iter).second->children.reserve(nodes.size()-start);
    for (size_t j=start; j<nodes.size(); j++)
      (*iter).second->children.push_back(&nodes[j]);

    dfs(&nodes[0]);

    /**
    for (size_t i=0; i<nodes.size(); i++)
      cout << nodes[i].parent_id << "," << nodes[i].tax_id << "\n";
    */
    cout << flush;
  }
  catch (std::exception &e) {
    cerr << "Error - " << e.what() << endl;
    exit(1);
  }
}
