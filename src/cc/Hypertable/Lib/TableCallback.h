/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_TABLECALLBACK_H
#define HYPERTABLE_TABLECALLBACK_H

#include "ResultCallback.h"

namespace Hypertable {

  class TableScanner;
  class TableMutator;

  /** Represents an open table.
   */
  class TableCallback: public ResultCallback {

  public:

    TableCallback(TableScanner *scanner) : m_scanner(scanner) {};
    TableCallback(TableMutator *mutator) : m_mutator(mutator) {};

    /**
     * Callback method for successful scan
     *
     * @param scanner
     * @param cells returned cells
     */
    void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells);

    /**
     * Callback method for scan errors
     *
     * @param scanner
     * @param error
     * @param error_msg
     * @param eos end of scan
     */
    void scan_error(TableScannerAsync *scanner, int error, const String &error_msg,
                    bool eos);

    /**
     * Callback method for successful mutations
     *
     * @param mutator
     */
    void update_ok(TableMutatorAsync *mutator);

    /**
     * Callback method for mutation errors
     *
     * @param mutator
     * @param error
     * @param failures vector of failed mutations
     */
    void update_error(TableMutatorAsync *mutator, int error, FailedMutations &failures);

  private:
    TableScanner *m_scanner;
    TableMutator *m_mutator;

  };
  typedef intrusive_ptr<TableCallback> TableCallbackPtr;
} // namesapce Hypertable

#endif // HYPERTABLE_TABLECALLBACK_H
