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

/** @file
 * Type declarations for PseudoTables class.
 * This file contains the type declarations for PseudoTables, a singleton class
 * that holds Schema objects for the pseudo tables.
 */

#ifndef Hypertable_Lib_PseudoTables_h
#define Hypertable_Lib_PseudoTables_h

#include "Schema.h"

#include <mutex>

namespace Hypertable {

  /** @addtogroup libHypertable
   * @{
   */

  /** Singleton class holding Schema objects for pseudo tables.
   */
  class PseudoTables {

  public:

    /** Enumeration for pseudo table column family codes.
     */
    enum ColumnFamilyCode {
      /// <i>.cellstore.index</i> <code>Size</code> column family
      CELLSTORE_INDEX_SIZE = 1,
      /// <i>.cellstore.index</i> <code>CompressedSize</code> column family
      CELLSTORE_INDEX_COMPRESSED_SIZE = 2,
      /// <i>.cellstore.index</i> <code>KeyCount</code> column family
      CELLSTORE_INDEX_KEY_COUNT = 3
    };

    /** Creates and/or returns singleton instance of the PseudoTables class.
     * This method will construct a new instance of the PseudoTables class
     * if it has not already been created.  All calls to this method return a
     * pointer to the same singleton instance of the PseudoTables object between
     * calls to #destroy.
     */
    static PseudoTables *instance() {
      std::lock_guard<std::mutex> lock(ms_mutex);

      if (!ms_instance)
        ms_instance = new PseudoTables();

      return ms_instance;
    }

    /** Destroys singleton instance of the PseudoTables class.
     */
    static void destroy() {
      std::lock_guard<std::mutex> lock(ms_mutex);
      if (ms_instance) {
        delete ms_instance;
        ms_instance = 0;
      }
    }

    Schema *cellstore_index;  //!< Schema of cellstore.index pseudo table

  private:

    /// Constructor (private).
    PseudoTables();

    /// Destructor (private).
    ~PseudoTables();

    /// Pointer to singleton instance of this class
    static PseudoTables *ms_instance;

    /// Mutex for serializing access to ms_instance
    static std::mutex ms_mutex;
  };

  /** @}*/
}

#endif // Hypertable_Lib_PseudoTables_h
