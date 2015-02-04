/* -*- c++ -*-
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
 * Declarations for MaintenanceFlag
 * This file contains declarations that are part of the MaintenanceFlag
 * namespace and provide classes and functions for manipulating a bit field
 * representing a set of maintenance types.
 */

#ifndef HYPERTABLE_MAINTENANCEFLAG_H
#define HYPERTABLE_MAINTENANCEFLAG_H

#include <unordered_map>

namespace Hypertable {

  /** @addtogroup RangeServer
   * @{
   */

  /** Maintenance task bit field.
   * Contains classes and functions for manipulating a bit field of maintenance
   * types.
   */
  namespace MaintenanceFlag {

    /** Enumeration for maintenance masks */
    enum {
      SPLIT                     = 0x00000100, //!< Split mask
      COMPACT                   = 0x00000200, //!< Compaction mask
      COMPACT_MINOR             = 0x00000201, //!< Minor compaction mask
      COMPACT_MAJOR             = 0x00000202, //!< Major compaction mask
      COMPACT_MERGING           = 0x00000204, //!< Mergin compaction mask
      COMPACT_GC                = 0x00000208, //!< GC compaction mask
      COMPACT_MOVE              = 0x00000210, //!< Merging compaction mask
      MEMORY_PURGE              = 0x00000400, //!< Memory purge mask
      MEMORY_PURGE_SHADOW_CACHE = 0x00000401, //!< Memory shadow cache purge mask
      MEMORY_PURGE_CELLSTORE    = 0x00000402, //!< Memory cellstore index purge mask
      RELINQUISH                = 0x00000800,  //!< Relinquish mask
      /// Recompute %CellStore merge run to test if merging compaction needed
      RECOMPUTE_MERGE_RUN = 0x00010000
    };

    /** Tests the RELINQUISH bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if RELINQUISH bit is set, <i>false</i>
     * otherwise.
     */
    inline bool relinquish(int flags) {
      return (flags & RELINQUISH) == RELINQUISH;
    }

    /** Tests the SPLIT bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if SPLIT bit is set, <i>false</i>
     * otherwise.
     */
    inline bool split(int flags) {
      return (flags & SPLIT) == SPLIT;
    }

    /** Tests the COMPACT bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if COMPACT bit is set, <i>false</i>
     * otherwise.
     */
    inline bool compaction(int flags) {
      return (flags & COMPACT) == COMPACT;
    }

    /** Tests the COMPACT_MINOR bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if COMPACT_MINOR bit is set, <i>false</i>
     * otherwise.
     */
    inline bool minor_compaction(int flags) {
      return (flags & COMPACT_MINOR) == COMPACT_MINOR;
    }

    /** Tests the COMPACT_MERGING bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if COMPACT_MERGING bit is set, <i>false</i>
     * otherwise.
     */
    inline bool merging_compaction(int flags) {
      return (flags & COMPACT_MERGING) == COMPACT_MERGING;
    }

    /** Tests the COMPACT_MAJOR bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if COMPACT_MAJOR bit is set, <i>false</i>
     * otherwise.
     */
    inline bool major_compaction(int flags) {
      return (flags & COMPACT_MAJOR) == COMPACT_MAJOR;
    }

    /** Tests the COMPACT_GC bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if COMPACT_GC bit is set, <i>false</i>
     * otherwise.
     */
    inline bool gc_compaction(int flags) {
      return (flags & COMPACT_GC) == COMPACT_GC;
    }

    /** Tests the COMPACT_MOVE bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if COMPACT_MOVE bit is set, <i>false</i>
     * otherwise.
     */
    inline bool move_compaction(int flags) {
      return (flags & COMPACT_MOVE) == COMPACT_MOVE;
    }

    /** Tests the PURGE_SHADOW_CACHE bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if PURGE_SHADOW_CACHE bit is set, <i>false</i>
     * otherwise.
     */
    inline bool purge_shadow_cache(int flags) {
      return (flags & MEMORY_PURGE_SHADOW_CACHE) == MEMORY_PURGE_SHADOW_CACHE;
    }

    /** Tests the PURGE_CELLSTORE bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if PURGE_CELLSTORE bit is set, <i>false</i>
     * otherwise.
     */
    inline bool purge_cellstore(int flags) {
      return (flags & MEMORY_PURGE_CELLSTORE) == MEMORY_PURGE_CELLSTORE;
    }

    /** Tests the RECOMPUTE_MERGE_RUN bit of <code>flags</code>
     * @param flags Bit field of maintenance types
     * @return <i>true</i> if RECOMPUTE_MERGE_RUN bit is set, <i>false</i>
     * otherwise.
     */
    inline bool recompute_merge_run(int flags) {
      return (flags & RECOMPUTE_MERGE_RUN) == RECOMPUTE_MERGE_RUN;
    }

    /** Hash function class for pointers. */
    class Hash {
    public:
      size_t operator () (const void *obj) const {
	return (size_t)obj;
      }
    };

    /** Equality function for pointers. */
    struct Equal {
      bool operator()(const void *obj1, const void *obj2) const {
	return obj1 == obj2;
      }
    };


    /** Maps object pointers to bit fields.
     * When scheduling maintenance for a range, the maintenance scheduler
     * sometimes needs to schedule different maintenance tasks for different
     * AccessGroups and CellStores within the range.  This class was introduced
     * to carry per-AccessGroup and per-CellStore maintenance task information
     * from the scheduler to the Range.  It's defined generically, but is
     * used to map AccessGroup and CellStore pointers to corresponding bit
     * fields describing what maintenance needs to be performed on them.
     */
    class Map : public std::unordered_map<const void *, int, Hash, Equal> {
    public:
      /** Returns bit field for a give pointer
       * @param key Pointer for which to return bit field
       * @return Bit field correspoding to <code>key</code>
       */
      int flags(const void *key) {
	iterator iter = this->find(key);
	if (iter != this->end())
	  return (*iter).second;
	return 0;
      }
      /** Test if compaction needs to be perfomed on object.
       * @param key Pointer to object (AccessGroup) to test for compaction
       * @return <i>true</i> if compaction is to be performed on object,
       * <i>false</i> otherwise
       */
      bool compaction(const void *key) {
	iterator iter = this->find(key);
	if (iter != this->end())
	  return ((*iter).second & COMPACT) == COMPACT;
	return false;
      }
      /** Test if minor compaction needs to be perfomed on object.
       * @param key Pointer to object (AccessGroup) to test for minor compaction
       * @return <i>true</i> if minor compaction is to be performed on object,
       * <i>false</i> otherwise
       */
      bool minor_compaction(const void *key) {
	iterator iter = this->find(key);
	if (iter != this->end())
	  return ((*iter).second & COMPACT_MINOR) == COMPACT_MINOR;
	return false;
      }
      /** Test if memory purge needs to be perfomed on object.
       * @param key Pointer to object (AccessGroup) to test for memory purge
       * @return <i>true</i> if memory purge is to be performed on object,
       * <i>false</i> otherwise
       */
      bool memory_purge(const void *key) {
	iterator iter = this->find(key);
	if (iter != this->end())
	  return ((*iter).second & MEMORY_PURGE) == MEMORY_PURGE;
	return false;
      }
    };
  }

  /** @} */
}

#endif // HYPERTABLE_MAINTENANCEFLAG_H
