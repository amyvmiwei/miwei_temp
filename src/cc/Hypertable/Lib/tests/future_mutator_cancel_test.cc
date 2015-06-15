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

#include <Hypertable/Lib/Client.h>
#include <Hypertable/Lib/Future.h>

#include <Common/md5.h>
#include <Common/Usage.h>

#include <cstdlib>
#include <iostream>

using namespace std;
using namespace Hypertable;

namespace {

  const char *usage[] = {
    "usage: ./future_mutator_cancel_test num_mutators cells_per_flush total_cells",
    "",
    "Validates asynchronous (Future) API and cancellation.",
    "num_mutators Number of async mutators to use",
    "cells_per_flush Number of cells per mutator before flush",
    "total_cells Total number of cells before cancelling",
    0
  };

  void load_buffer_with_random(uint8_t *buf, size_t size) {
    uint8_t *ptr = buf;
    uint32_t uival;
    size_t n = size / 4;
    if (size % 4)
      n++;
    for (size_t i=0; i<n; i++) {
      uival = (uint32_t)random();
      memcpy(ptr, &uival, 4);
      ptr += 4;
    }
  }

}


int main(int argc, char **argv) {
  unsigned long seed = 1234;
  uint8_t *buf = new uint8_t [1048576];
  char keybuf[32];
  uint32_t num_mutators, cells_per_flush, total_cells;

  if ((argc > 1 && (!strcmp(argv[1], "-?") || !strcmp(argv[1], "--help"))) || argc != 4)
    Usage::dump_and_exit(usage);

  num_mutators = atoi(argv[1]);
  cells_per_flush = atoi(argv[2]);
  total_cells = atoi(argv[3]);

  cout << "num_mutators=" << num_mutators << ", cells_per_flush=" << cells_per_flush
       << ", total_cells=" << total_cells << endl;

  srandom(seed);

  try {
    Client *hypertable = new Client(argv[0], "./future_test.cfg");
    NamespacePtr ns = hypertable->open_namespace("/");

    TablePtr table_ptr;
    KeySpec key;
    ScanSpecBuilder ssbuilder;
    ScanSpec scan_spec;
    Cell cell;
    ResultPtr result;
    Cells cells;

    /**
     * Load data
     */
    {
      Future ff;
      vector<TableMutatorAsyncPtr> mutator_ptrs;
      table_ptr = ns->open_table("FutureTest");

      for(size_t ii=0; ii<num_mutators; ++ii)
        mutator_ptrs.push_back(TableMutatorAsyncPtr(table_ptr->create_mutator_async(&ff)));

      key.column_family = "data";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;

      size_t cells=0;
      size_t ii=0;
      while(true) {
        for (size_t jj=0; jj < num_mutators; ++jj) {
          load_buffer_with_random(buf, 10);
          sprintf(keybuf, "%05u", (unsigned)cells);
          key.row = keybuf;
          key.row_len = strlen(keybuf);
          mutator_ptrs[jj]->set(key, buf, 10);
          if (ii % cells_per_flush == 0)
            mutator_ptrs[jj]->flush();
        }
        cells++;
        if (cells >= total_cells) {
          HT_INFOF("Wrote %u cells, cancelling", (unsigned)cells);
          ff.cancel();
          break;
        }
        ii++;
      }
      HT_INFO("Cancel future test finished");
    }

    // this time cancel individual mutators
    if (num_mutators > 1) {
      Future ff;
      vector<TableMutatorAsyncPtr> mutator_ptrs;
      set<TableMutatorAsync *> cancelled_mutator_ptrs;
      table_ptr = ns->open_table("FutureTest");

      for(size_t ii=0; ii<num_mutators; ++ii)
        mutator_ptrs.push_back(TableMutatorAsyncPtr(table_ptr->create_mutator_async(&ff)));

      key.column_family = "data";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;

      size_t cells=0;
      size_t num_updates=0;
      while(true) {
        for (size_t jj=0; jj < num_mutators; ++jj) {
          TableMutatorAsyncPtr mutator_ptr = mutator_ptrs[jj];
          if (cancelled_mutator_ptrs.find(mutator_ptr.get()) != cancelled_mutator_ptrs.end())
            continue;
          load_buffer_with_random(buf, 10);
          sprintf(keybuf, "%05u", (unsigned)cells);
          key.row = keybuf;
          key.row_len = strlen(keybuf);
          mutator_ptr->set(key, buf, 10);
          if (cells % cells_per_flush == 0)
            mutator_ptr->flush();
          size_t cancel_after_num_cells = total_cells / 4 + (random() % total_cells*3/4);
          if (cancelled_mutator_ptrs.size() < num_mutators/2
              && cells >= cancel_after_num_cells) {
            HT_INFOF("Cancelling mutator #%llu", (Llu)jj);
            mutator_ptr->cancel();
            cancelled_mutator_ptrs.insert(mutator_ptr.get());
          }
        }
        while (!ff.is_empty() && ff.get(result)) {
          if (result->is_error()) {
            int error;
            String error_msg;
            result->get_error(error, error_msg);
            Exception e(error, error_msg);
            HT_ERROR_OUT << "Encountered mutate error " << e << HT_END;
            quick_exit(EXIT_FAILURE);
          }
          else if (cancelled_mutator_ptrs.find(result->get_mutator()) != cancelled_mutator_ptrs.end()) {
            HT_ERROR("Result from a cancelled scanner received");
            quick_exit(EXIT_FAILURE);
          }
          else
            ++num_updates;
        }
        if (cells >= total_cells) {
          HT_INFOF("Write %lu cells, got %lu updates, %lu/%lu mutators cancelled",
                   (unsigned long)cells, (unsigned long)num_updates,
                   (unsigned long)cancelled_mutator_ptrs.size(),
                   (unsigned long)num_mutators);
          ff.cancel();
          break;
        }
        ++cells;
      }
      HT_INFO("Cancel individual mutators test finished");
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    quick_exit(EXIT_FAILURE);
  }

  quick_exit(EXIT_SUCCESS);
}
