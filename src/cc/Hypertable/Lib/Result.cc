/**
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
#include "Result.h"

#include "TableMutatorAsync.h"
#include "TableScannerAsync.h"

namespace Hypertable {

Result::Result(TableScannerAsync *scanner, ScanCellsPtr &cells) : m_scanner(scanner),
         m_mutator(0), m_cells(cells), m_error(0), m_isscan(true), m_iserror(false) {
  return;
}

Result::Result(TableScannerAsync *scanner, int error, const String &error_msg) :
    m_scanner(scanner), m_mutator(0), m_error(error), m_error_msg(error_msg), m_isscan(true),
    m_iserror(true) {
  return;
}

Result::Result(TableMutatorAsync *mutator) : m_scanner(0), m_mutator(mutator),
                              m_error(0), m_isscan(false), m_iserror(false)  {
}

Result::Result(TableMutatorAsync *mutator, int error, FailedMutations &failed_mutations)
  : m_scanner(0), m_mutator(mutator), m_error(error), m_isscan(false), m_iserror(true) {
  m_failed_cells.copy_failed_mutations(failed_mutations, m_failed_mutations);
}

TableScannerAsync *Result::get_scanner() {
  if (!m_isscan)
    HT_THROW(Error::NOT_ALLOWED, "Requested scanner for non-scan result");
  return m_scanner;
}

TableMutatorAsync *Result::get_mutator() {
  if (m_isscan)
    HT_THROW(Error::NOT_ALLOWED, "Requested mutator for non-scan result");
  return m_mutator;
}

void Result::get_cells(Cells &cells) {
  if (!m_isscan)
    HT_THROW(Error::NOT_ALLOWED, "Requested scanspec for non-scan result");
  if (m_iserror)
    HT_THROW(Error::NOT_ALLOWED, "Requested Cells for scan error");
  m_cells->get(cells);
}

void Result::get_error(int &error, String &error_msg) {
  if (!m_iserror)
    HT_THROW(Error::NOT_ALLOWED, "Requested error for non-error result");
  error = m_error;
  error_msg = m_error_msg;
  return;
}

FailedMutations &
Result::get_failed_mutations() {
  return m_failed_mutations;
}

void Result::get_failed_cells(Cells &cells) {
  if (m_isscan)
    HT_THROW(Error::NOT_ALLOWED, "Requested failed cells for non-mutator result");
  if (!m_iserror)
    HT_THROW(Error::NOT_ALLOWED, "Requested failed cells for successful mutation");
  m_failed_cells.get(cells);
}
} // namespace Hypertable
