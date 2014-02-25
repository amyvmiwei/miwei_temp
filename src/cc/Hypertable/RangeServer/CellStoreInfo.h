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

#ifndef HYPERTABLE_CELLSTOREINFO_H
#define HYPERTABLE_CELLSTOREINFO_H

#include "CellCache.h"
#include "CellStoreV6.h"

namespace Hypertable {

  class CellStoreInfo {
  public:
    CellStoreInfo(CellStore *csp) :
      cs(csp), shadow_cache_ecr(TIMESTAMP_MAX), shadow_cache_hits(0), bloom_filter_accesses(0),
      bloom_filter_maybes(0), bloom_filter_fps(0) {
      init_from_trailer();
    }
    CellStoreInfo(CellStorePtr &csp) :
      cs(csp), shadow_cache_ecr(TIMESTAMP_MAX), shadow_cache_hits(0), bloom_filter_accesses(0),
      bloom_filter_maybes(0), bloom_filter_fps(0) {
      init_from_trailer();
    }
    CellStoreInfo(CellStorePtr &csp, CellCachePtr &scp, int64_t ecr) :
      cs(csp), shadow_cache(scp), shadow_cache_ecr(ecr), shadow_cache_hits(0),
      bloom_filter_accesses(0), bloom_filter_maybes(0), bloom_filter_fps(0)  {
      init_from_trailer();
    }
    CellStoreInfo() : cell_count(0), shadow_cache_ecr(TIMESTAMP_MAX),
                      shadow_cache_hits(0), bloom_filter_accesses(0),
                      bloom_filter_maybes(0), bloom_filter_fps(0) { }

    void init_from_trailer() {

      try {
        m_divisor = (boost::any_cast<uint32_t>(cs->get_trailer()->get("flags")) & CellStoreTrailerV6::SPLIT) ? 2 : 1;
      }
      catch (std::exception &e) {
        m_divisor = 1;
      }

      try {
        cell_count = boost::any_cast<int64_t>(cs->get_trailer()->get("total_entries")) / m_divisor;
        timestamp_min = boost::any_cast<int64_t>(cs->get_trailer()->get("timestamp_min"));
        timestamp_max = boost::any_cast<int64_t>(cs->get_trailer()->get("timestamp_max"));
      }
      catch (std::exception &e) {
        cell_count = 0;
        timestamp_min = TIMESTAMP_MAX;
        timestamp_max = TIMESTAMP_MIN;
      }
      try {
        key_bytes = boost::any_cast<int64_t>(cs->get_trailer()->get("key_bytes")) / m_divisor;
        value_bytes = boost::any_cast<int64_t>(cs->get_trailer()->get("value_bytes")) /m_divisor;
      }
      catch (std::exception &e) {
        key_bytes = value_bytes = 0;
      }
    }

    int64_t expirable_data() {
      try {
        return boost::any_cast<int64_t>(cs->get_trailer()->get("expirable_data")) / m_divisor;
      }
      catch (std::exception &e) {
        return 0;
      }
    }

    int64_t delete_count() {
      try {
        return boost::any_cast<int64_t>(cs->get_trailer()->get("delete_count")) / m_divisor;
      }
      catch (std::exception &e) {
        return 0;
      }
    }

    CellStorePtr cs;
    CellCachePtr shadow_cache;
    uint64_t cell_count;
    int64_t key_bytes;
    int64_t value_bytes;
    int64_t shadow_cache_ecr;
    uint32_t shadow_cache_hits;
    uint32_t bloom_filter_accesses;
    uint32_t bloom_filter_maybes;
    uint32_t bloom_filter_fps;
    int64_t timestamp_min;
    int64_t timestamp_max;
    int64_t total_data;

  private:
    int m_divisor {1};
  };

} // namespace Hypertable

#endif // HYPERTABLE_CELLSTOREINFO_H
