/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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
 * Collecting and (de)serializing system-wide statistics.
 */

#ifndef HYPERTABLE_STATSSYSTEM_H
#define HYPERTABLE_STATSSYSTEM_H

#include "Common/StatsSerializable.h"
#include "Common/SystemInfo.h"

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * Collects, serializes and deserializes system-wide statistics.
   */
  class StatsSystem : public StatsSerializable {
  public:
    /** All statistics categories that are provided by this class */
    enum Category {
      CPUINFO  = 0x0001,
      CPU      = 0x0002,
      LOADAVG  = 0x0004,
      MEMORY   = 0x0008,
      DISK     = 0x0010,
      SWAP     = 0x0020,
      NETINFO  = 0x0040,
      NET      = 0x0080,
      OSINFO   = 0x0100,
      PROCINFO = 0x0200,
      PROC     = 0x0400,
      FS       = 0x0800,
      TERMINFO = 0x1000
    };

    /** Default constructor; creates an empty object */
    StatsSystem()
      : StatsSerializable(SYSTEM, 0), m_categories(0) {
    }

    /** Constructor; collects the specified categories
     *
     * @param categories bit-wise ORed categories */
    StatsSystem(int32_t categories);

    /** Constructor; collects the specified categories
     *
     * @param categories bit-wise ORed categories
     * @param dirs Vector of data directories which are monitored
     */
    StatsSystem(int32_t categories, std::vector<String> &dirs);

    /** Adds more categories to the already existing categories */
    void add_categories(int32_t categories);

    /** Adds more categories and data directories */
    void add_categories(int32_t categories, std::vector<String> &dirs);

    /** Refreshes the statistics */
    void refresh();

    /** Equal operator */
    bool operator==(const StatsSystem &other) const;

    /** Not Equal operator */
    bool operator!=(const StatsSystem &other) const {
      return !(*this == other);
    }
    
    /** CPU information (vendor, model, cores, cache sizes etc) */
    struct CpuInfo cpu_info;

    /** CPU statistics (user load, system load, idle etc) */
    struct CpuStat cpu_stat;

    /** Load average normalized over number of cores */
    struct LoadAvgStat loadavg_stat;

    /** Memory statistics (total size, free space, used etc) */
    struct MemStat mem_stat;

    /** Swapping statistics (page in, page out etc) */
    struct SwapStat swap_stat;

    /** Network information (host name, primary interface, gateway etc) */
    struct NetInfo net_info;

    /** Network statistics (transfer rates, receiving rate etc) */
    struct NetStat net_stat;

    /** OS information (name, architecture, version etc) */
    struct OsInfo os_info;

    /** Process information (pid, user, working directory etc) */
    struct ProcInfo proc_info;

    /** Process statistics (CPU user time, system time, vm size etc) */
    struct ProcStat proc_stat;

    /** Terminal information (number of lines, number of columns etc) */
    struct TermInfo term_info;

    /** Per-Disk statistics (read-rate, write-rate, etc) */
    std::vector<struct DiskStat> disk_stat;

    /** Filesystem statistics (free space, used space, aggregate files etc) */
    std::vector<struct FsStat> fs_stat;

  protected:
    /** Returns the encoded length of a statistics group */
    virtual size_t encoded_length_group(int group) const;

    /** Serializes a statistics group to memory */
    virtual void encode_group(int group, uint8_t **bufp) const;

    /** Deserializes a statistics group from memory */
    virtual void decode_group(int group, uint16_t len, const uint8_t **bufp,
            size_t *remainp);
    
  private:
    int32_t m_categories;
  };

  typedef intrusive_ptr<StatsSystem> StatsSystemPtr;

  /** @} */

}

#endif // HYPERTABLE_STATSSYSTEM_H
