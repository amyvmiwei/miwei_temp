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

#ifndef Hypertable_Lib_IndexScannerCallback_h
#define Hypertable_Lib_IndexScannerCallback_h

#include <Hypertable/Lib/Client.h>
#include <Hypertable/Lib/LoadDataEscape.h>
#include <Hypertable/Lib/Namespace.h>
#include <Hypertable/Lib/ResultCallback.h>
#include <Hypertable/Lib/ScanSpec.h>
#include <Hypertable/Lib/TableScannerAsync.h>

#include <HyperAppHelper/Unique.h>

#include <Common/Filesystem.h>
#include <Common/FlyweightString.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

// this macro enables the "ScanSpecBuilder queue" test code; it fills the queue
// till it exceeds the limit, and makes sure that the queue is blocking
// till it gets empty again
#undef TEST_SSB_QUEUE

namespace {
  struct QualifierFilterMatch :
    std::unary_function<std::pair<String, String>, bool> {
    QualifierFilterMatch(const char *row) : row(row) { }
    bool operator()(const std::pair<String, String> &filter) const {
      if (!strncmp(filter.first.c_str(), row, filter.first.length())) {
        if (filter.second.empty() ||
            strstr(row+filter.first.length(), filter.second.c_str()))
          return true;
      }
      return false;
    }
    const char *row;
  };
}

namespace Hypertable {

  static const char *tmp_schema_outer=
        "<Schema>"
          "<AccessGroup name=\"default\">"
            "%s"
          "</AccessGroup>"
        "</Schema>";

  static const char *tmp_schema_inner=
        "<ColumnFamily>"
          "<Name>%s</Name>"
          "<Counter>false</Counter>"
          "<deleted>false</deleted>"
        "</ColumnFamily>";

  /** ResultCallback for secondary indices; used by TableScannerAsync
   */
  class IndexScannerCallback : public ResultCallback {

#if defined (TEST_SSB_QUEUE)
    static const size_t SSB_QUEUE_LIMIT = 4;
#else
    static const size_t SSB_QUEUE_LIMIT = 40;
#endif

    /** if more than TMP_CUTOFF bytes are received from the index then
     * store all results in a temporary table */
#if defined (TEST_SSB_QUEUE)
    static const size_t TMP_CUTOFF = 1;
#else
    static const size_t TMP_CUTOFF = 16*1024*1024; // FIXME: Config property?
#endif

  public:

    IndexScannerCallback(TableScannerAsync* primary_scanner, Table *primary_table,
                         const ScanSpec &primary_spec,
                         std::vector<CellPredicate> &cell_predicates,
                         ResultCallback *original_cb, uint32_t timeout_ms, 
                         bool qualifier_scan,
                         bool row_intervals_applied)
      : ResultCallback(), m_primary_scanner(primary_scanner),
        m_primary_table(primary_table), m_primary_spec(primary_spec),
        m_original_cb(original_cb), m_timeout_ms(timeout_ms),
        m_qualifier_scan(qualifier_scan),
        m_row_intervals_applied(row_intervals_applied) {
      m_original_cb->increment_outstanding();

      m_cell_predicates.swap(cell_predicates);

      if (primary_spec.row_limit != 0 ||
          primary_spec.cell_limit != 0 ||
          primary_spec.row_offset != 0 ||
          primary_spec.cell_offset != 0 ||
          primary_spec.cell_limit_per_family != 0) {
        // keep track of offset and limit
        m_track_limits = true;
        m_row_limit = primary_spec.row_limit;
        m_cell_limit = primary_spec.cell_limit;
        m_row_offset = primary_spec.row_offset;
        m_cell_offset = primary_spec.cell_offset;
        m_cell_limit_per_family = primary_spec.cell_limit_per_family;
      }
      else
        m_track_limits = false;

      // Setup bit pattern for all matching predicates
      for (size_t i=0; i<primary_spec.column_predicates.size(); i++) {
        m_all_matching <<= 1;
        m_all_matching |= 1;
      }

      for (auto cf : primary_table->schema()->get_column_families()) {
        if (!cf->get_value_index() && !cf->get_qualifier_index())
          continue;
        m_column_map[cf->get_id()] = cf->get_name();
      }
    }

    virtual ~IndexScannerCallback() {
      std::lock_guard<std::mutex> lock1(m_scanner_mutex);
      std::lock_guard<std::mutex> lock2(m_mutex);
      m_scanners.clear();
      sspecs_clear();
      if (m_tmp_table) {
        Client *client = m_primary_table->get_namespace()->get_client();
        NamespacePtr nstmp = client->open_namespace("/tmp");
        nstmp->drop_table(Filesystem::basename(m_tmp_table->get_name()), true);
      }
    }

    void shutdown() {
      {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_shutdown = true;
      }

      {
        std::lock_guard<std::mutex> lock(m_scanner_mutex);
        for (auto s : m_scanners)
          delete s;
      }

      wait_for_completion();

    }

    void sspecs_clear() {
      for (auto ssb : m_sspecs)
        delete ssb;
      m_sspecs.clear();
      m_sspecs_cond.notify_one();
    }

    /**
     * Callback method for successful scan
     *
     * @param scanner Pointer to scanner
     * @param scancells returned cells
     */
    virtual void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &scancells) {
      bool is_eos = scancells->get_eos();
      std::string table_name = scanner->get_table_name();

      std::unique_lock<std::mutex> lock(m_mutex);

      // ignore empty packets
      if (scancells->get_eos() == false && scancells->empty())
        return;

      // reached end of this scanner?
      if (is_eos) {
        HT_ASSERT(m_outstanding_scanners.load() > 0);
        m_outstanding_scanners--;
      }

      // we've reached eos (i.e. because CELL_LIMITs/ROW_LIMITs were reached)
      // just collect the outstanding scanners and ignore the cells
      if (m_eos == true) {
        if (m_outstanding_scanners.load() == 0)
          final_decrement(is_eos);
        return;
      }

      // If the cells are from the index table then collect and store them
      // in memory (or in a temporary table)
      if (Filesystem::basename(table_name)[0] == '^')
        collect_indices(scanner, scancells);
      // If the cells are from the temporary table then they need to be 
      // verified against the primary table
      else if (table_name != m_primary_table->get_name())
        verify_results(lock, scanner, scancells);
      // Otherwise cells are returned from the primary table: check 
      // LIMIT/OFFSET and send them to the original callback
      else {
        scancells->set_eos(false);

        if (m_track_limits)
          track_predicates(scancells);
        else
          m_original_cb->scan_ok(m_primary_scanner, scancells);

        // fetch data from the next scanner when we have reached the end of
        // the current one
        if (!m_limits_reached && is_eos)
          readahead();
      }

      final_decrement(is_eos);
    }

    virtual void register_scanner(TableScannerAsync *scanner) {
      m_outstanding_scanners++;
    }

    /**
     * Callback method for scan errors
     *
     * @param scanner
     * @param error
     * @param error_msg
     * @param eos end of scan
     */
    virtual void scan_error(TableScannerAsync *scanner, int error, 
                            const std::string &error_msg, bool eos) {
      m_original_cb->scan_error(scanner, error, error_msg, eos);
      if (eos)
        m_original_cb->decrement_outstanding();
    }

    virtual void update_ok(TableMutatorAsync *mutator) {
    }

    virtual void update_error(TableMutatorAsync *mutator, int error, 
                              FailedMutations &failures) {
      m_original_cb->update_error(mutator, error, failures);
    }

   private:
    void final_decrement(bool is_eos) {
      // If the last outstanding scanner just finished; send an "eos" 
      // packet to the original callback and decrement the outstanding scanners
      // once more (this is the equivalent operation to the increment in
      // the constructor)
      if ((is_eos || m_eos) && m_outstanding_scanners.load() == 0) {
        m_eos = true;
        HT_ASSERT(m_final_decrement == false);
        if (!m_final_decrement) {
          // send empty eos package to caller
          ScanCellsPtr empty = std::make_shared<ScanCells>();
          empty->set_eos();
          m_original_cb->scan_ok(m_primary_scanner, empty);
          m_original_cb->decrement_outstanding();
          m_final_decrement = true;
        }
      }
    }

    void collect_indices(TableScannerAsync *scanner, ScanCellsPtr &scancells) {
      const ScanSpec &primary_spec = m_primary_spec.get();
      // split the index row into column id, cell value and cell row key
      Cells cells;
      const char *unescaped_row;
      const char *unescaped_qualifier;
      const char *unescaped_value;
      size_t unescaped_row_len;
      size_t unescaped_qualifier_len;
      size_t unescaped_value_len;
      LoadDataEscape escaper_row;
      LoadDataEscape escaper_qualifier;
      LoadDataEscape escaper_value;
      scancells->get(cells);
      for (auto &cell : cells) {
        char *qv = (char *)cell.row_key;

        // get unescaped row
        char *row;
        if ((row = strrchr(qv, '\t')) == 0) {
          HT_WARNF("Invalid index entry '%s' in index table '^%s'",
                   qv, m_primary_table->get_name().c_str());
          continue;
        }
        *row++ = 0;
        escaper_row.unescape(row, strlen(row), &unescaped_row, &unescaped_row_len);

        // if the original query specified row intervals then these have
        // to be filtered in the client
        if (!m_row_intervals_applied && primary_spec.row_intervals.size()) {
          if (!row_intervals_match(primary_spec.row_intervals, unescaped_row))
            continue;
        }

        // cut off the "%d," part at the beginning to get the column id
        // The max. column id is 255, therefore there must be a ',' after 3
        // positions
        char *id = qv;
        while (*qv != ',' && (qv - id <= 4))
          qv++;
        if (*qv != ',') {
          HT_WARNF("Invalid index entry '%s' in index table '^%s'",
                  id, m_primary_table->get_name().c_str());
          continue;
        }
        *qv++ = 0;
        uint32_t cfid = (uint32_t)atoi(id);
        if (!cfid || m_column_map.find(cfid) == m_column_map.end()) {
          HT_WARNF("Invalid index entry '%s' in index table '^%s'",
                   qv, m_primary_table->get_name().c_str());
          continue;
        }

        // same about cell intervals
        if (primary_spec.cell_intervals.size()) {
          if (!cell_intervals_match(primary_spec.cell_intervals, unescaped_row,
                                    m_column_map[cfid].c_str()))
            continue;
        }

        uint32_t matching = 0;

        if (m_qualifier_scan) {
          escaper_qualifier.unescape(qv, strlen(qv),
                                     &unescaped_qualifier, &unescaped_qualifier_len);

          if (primary_spec.and_column_predicates) {
            std::bitset<32> bits;
            m_cell_predicates[cfid].all_matches(unescaped_qualifier,
                                                unescaped_qualifier_len,
                                                "", 0, bits);
            if ((matching = (uint32_t)bits.to_ulong()) == 0L)
              continue;
            
          }
          else if (!m_cell_predicates[cfid].matches(unescaped_qualifier,
                                                    unescaped_qualifier_len, "", 0))
            continue;
        }
        else {
          char *value = qv;
          if ((qv = strchr(value, '\t')) == 0) {
            HT_WARNF("Invalid index entry '%s' in index table '^%s'",
                     value, m_primary_table->get_name().c_str());
            continue;
          }
          size_t value_len = qv-value;
          *qv++ = 0;
          escaper_qualifier.unescape(qv, strlen(qv),
                                     &unescaped_qualifier, &unescaped_qualifier_len);
          escaper_value.unescape(value, value_len,
                                 &unescaped_value, &unescaped_value_len);
          if (primary_spec.and_column_predicates) {
            std::bitset<32> bits;
            m_cell_predicates[cfid].all_matches(unescaped_qualifier,
                                                unescaped_qualifier_len,
                                                unescaped_value,
                                                unescaped_value_len,
                                                bits);
            if ((matching = (uint32_t)bits.to_ulong()) == 0L)
              continue;

          }
          else if (!m_cell_predicates[cfid].matches(unescaped_qualifier,
                                                    unescaped_qualifier_len,
                                                    unescaped_value,
                                                    unescaped_value_len))
            continue;
        }

        // if a temporary table was already created then store it in the 
        // temporary table. otherwise buffer it in memory but make sure
        // that no duplicate rows are inserted
        KeySpec key;
        key.row = m_strings.get(unescaped_row);
        key.row_len = unescaped_row_len;
        key.column_family = m_column_map[cfid].c_str();
        key.timestamp = cell.timestamp;
        if (m_qualifier_scan) {
          key.column_qualifier = m_strings.get(unescaped_qualifier);
          key.column_qualifier_len = unescaped_qualifier_len;
        }
        if (m_mutator)
          m_mutator->set(key, &matching, sizeof(matching));
        else {
          CkeyMap::iterator it = m_tmp_keys.find(key);
          if (it == m_tmp_keys.end())
            m_tmp_keys.insert(CkeyMap::value_type(key, matching));
          else
            it->second |= matching;
          m_tmp_cutoff += sizeof(KeySpec) + key.row_len + key.column_qualifier_len;
        }
      }

      // reached EOS? then flush the mutator
      if (scancells->get_eos()) {
        if (m_mutator)
          m_mutator.reset();
        if (!m_tmp_table && m_tmp_keys.empty()) {
          m_eos = true;
          return;
        }
      }
      // not EOS? then more keys will follow
      else {
        // store all buffered keys in a temp. table if we have too many
        // results from the index
        if (!m_tmp_table && m_tmp_cutoff > TMP_CUTOFF) {
          create_temp_table();
          for (CkeyMap::iterator it = m_tmp_keys.begin(); 
                  it != m_tmp_keys.end(); ++it) 
            m_mutator->set(it->first, &it->second, sizeof(it->second));
        }
        // if a temp table existed (or was just created): clear the buffered
        // keys. they're no longer required
        if (m_tmp_table) {
          m_tmp_keys.clear();
          m_strings.clear();
        }

        return;
      }

      // we've reached EOS. If there's a temporary table then create a 
      // scanner for this table. Otherwise immediately send the temporary
      // results to the primary table for verification
      ScanSpecBuilder ssb;

      std::lock_guard<std::mutex> lock(m_scanner_mutex);
      if (m_shutdown)
        return;

      TableScannerAsync *s;
      if (m_tmp_table) {
        s = m_tmp_table->create_scanner_async(this, ssb.get(), 
                m_timeout_ms, Table::SCANNER_FLAG_IGNORE_INDEX);
      }
      else {
        ssb.set_max_versions(primary_spec.max_versions);
        ssb.set_return_deletes(primary_spec.return_deletes);
        ssb.set_keys_only(primary_spec.keys_only);
        ssb.set_row_regexp(primary_spec.row_regexp);

        // Fetch primary columns and restrict by time interval
        for (auto col : primary_spec.columns)
          ssb.add_column(col);
        ssb.set_time_interval(primary_spec.time_interval.first, 
                              primary_spec.time_interval.second);

        const char *last_row = "";

        if (primary_spec.and_column_predicates) {

          // Add row interval for each entry returned from index
          for (CkeyMap::iterator it = m_tmp_keys.begin(); 
               it != m_tmp_keys.end(); ++it) {
            if (strcmp((const char *)it->first.row, last_row)) {
              if (m_cur_matching == m_all_matching)
                ssb.add_row(last_row);
              last_row = (const char *)it->first.row;
              m_cur_matching = it->second;
            }
            else
              m_cur_matching |= it->second;
          }
          if (m_cur_matching == m_all_matching)
            ssb.add_row(last_row);
          m_cur_matching = 0;
        }
        else {
          for (CkeyMap::iterator it = m_tmp_keys.begin(); 
               it != m_tmp_keys.end(); ++it) {
            if (strcmp((const char *)it->first.row, last_row)) {
              if (*last_row)
                ssb.add_row(last_row);
              last_row = (const char *)it->first.row;
            }
          }
          if (*last_row)
            ssb.add_row(last_row);
        }

        if (ssb.get().row_intervals.empty()) {
          m_eos = true;
          return;
        }

        ssb.set_scan_and_filter_rows(primary_spec.scan_and_filter_rows);

        s = m_primary_table->create_scanner_async(this, ssb.get(), 
                m_timeout_ms, Table::SCANNER_FLAG_IGNORE_INDEX);

        // clean up
        m_tmp_keys.clear();
        m_strings.clear();
      }

      m_scanners.push_back(s);
    }

    /*
     * the temporary table mimicks the primary table: all column families
     * with an index are also created for the temporary table
     */
    void create_temp_table() {
      HT_ASSERT(!m_tmp_table);
      HT_ASSERT(!m_mutator);

      std::string inner;
      for (auto cf : m_primary_table->schema()->get_column_families()) {
        if (m_qualifier_scan && !cf->get_qualifier_index())
          continue;
        if (!m_qualifier_scan && !cf->get_value_index())
          continue;
        inner += format(tmp_schema_inner, cf->get_name().c_str());
      }

      Client *client = m_primary_table->get_namespace()->get_client();
      NamespacePtr nstmp( client->open_namespace("/tmp") );
      std::string guid = HyperAppHelper::generate_guid();
      nstmp->create_table(guid, format(tmp_schema_outer, inner.c_str()));
      m_tmp_table = nstmp->open_table(guid);

      m_mutator.reset(m_tmp_table->create_mutator_async(this));
    }

    void verify_results(std::unique_lock<std::mutex> &lock, TableScannerAsync *scanner, 
                        ScanCellsPtr &scancells) {
      // no results from the primary table, or LIMIT/CELL_LIMIT exceeded? 
      // then return immediately
      if ((scancells->get_eos() && scancells->empty() &&
           m_last_rowkey_verify.empty()) || m_limits_reached) {
        sspecs_clear();
        m_eos = true;
        return;
      }

      const ScanSpec &primary_spec = m_primary_spec.get();

      Cells cells;
      scancells->get(cells);
      const char *last = m_last_rowkey_verify.c_str();

      // this test code creates one ScanSpec for each single row that is 
      // received from the temporary table. As soon as the scan spec queue
      // overflows it will block till the primary table scanners clear the
      // queue.
      //
      // see below for more comments
#if defined (TEST_SSB_QUEUE)
      for (auto &cell : cells) {
        if (!strcmp(last, (const char *)cell.row_key))
          continue;
        last = (const char *)cell.row_key;

        ScanSpecBuilder *ssb = new ScanSpecBuilder;
        for (const auto &s : primary_spec.columns)
          ssb->add_column(s.c_str());
        ssb->set_max_versions(primary_spec.max_versions);
        ssb->set_return_deletes(primary_spec.return_deletes);
        for (const auto &cp : primary_spec.column_predicates)
          ssb->add_column_predicate(cp.column_family, cp.operation,
                  cp.value, cp.value_len);
        if (primary_spec.value_regexp)
          ssb->set_value_regexp(primary_spec.value_regexp);

        ssb->add_row(cell.row_key);

        m_last_rowkey_verify = last;

        m_sspecs_cond.wait(lock, [this](){
            return m_sspecs.size() <= SSB_QUEUE_LIMIT || m_limits_reached; });

        if (m_limits_reached) { 
          delete ssb;
          return;
        }

        m_sspecs.push_back(ssb);
        if (m_outstanding_scanners.load() <= 1)
          readahead();
      }
#else
      // This is the "production-ready" code, using a single ScanSpec for all 
      // rows that are returned from the intermediate table
      //
      // Create a new ScanSpec
      ScanSpecBuilder *ssb = new ScanSpecBuilder;
      for (auto col : primary_spec.columns)
        ssb->add_column(col);
      ssb->set_max_versions(primary_spec.max_versions);
      ssb->set_return_deletes(primary_spec.return_deletes);
      ssb->set_keys_only(primary_spec.keys_only);
      if (primary_spec.value_regexp)
        ssb->set_value_regexp(primary_spec.value_regexp);

      uint32_t matching;

      // foreach_ht cell from the secondary index: verify that it exists in
      // the primary table, but make sure that each rowkey is only inserted
      // ONCE
      if (primary_spec.and_column_predicates) {
        for (auto &cell : cells) {

          HT_ASSERT(cell.value_len == sizeof(matching));
          memcpy(&matching, cell.value, sizeof(matching));

          if (!strcmp(last, (const char *)cell.row_key)) {
            m_cur_matching |= matching;
            continue;
          }

          // then add the key to the ScanSpec
          if (m_cur_matching == m_all_matching)
            ssb->add_row(last);

          last = (const char *)cell.row_key;

          m_cur_matching = matching;
        }
        if (scancells->get_eos() && m_cur_matching == m_all_matching) {
          m_cur_matching = 0;
          ssb->add_row(last);
          last = "";
        }
      }
      else {
        for (auto &cell : cells) {
          if (!strcmp(last, (const char *)cell.row_key))
            continue;
          // then add the key to the ScanSpec
          ssb->add_row(last);
          last = (const char *)cell.row_key;
        }
        if (scancells->get_eos()) {
          ssb->add_row(last);
          last = "";
        }
      }

      ssb->set_scan_and_filter_rows(primary_spec.scan_and_filter_rows);
 
      // store the "last" pointer before it goes out of scope
      m_last_rowkey_verify = last;

      m_sspecs_cond.wait(lock, [this](){
          return m_sspecs.size() <= SSB_QUEUE_LIMIT || m_limits_reached; });

      // if, in the meantime, we reached any CELL_LIMIT/ROW_LIMIT then return
      if (m_limits_reached || ssb->get().row_intervals.empty()) { 
        delete ssb;
        return;
      }

      // store ScanSpec in the queue
      m_sspecs.push_back(ssb);

      // there should always at least be two scanners outstanding: this scanner
      // from the intermediate table and one scanner from the primary table.
      // If not then make sure to start another readahead scanner on the
      // primary table.
      if (m_outstanding_scanners.load() <= 0)
        readahead();
#endif
    }

    void readahead() {
      HT_ASSERT(m_limits_reached == false);
      HT_ASSERT(m_eos == false);

      if (m_sspecs.empty())
        return;

      ScanSpecBuilder *ssb = m_sspecs[0];
      m_sspecs.pop_front();
      if (m_shutdown) {
        delete ssb;
        return;
      }
      TableScannerAsync *s = 
            m_primary_table->create_scanner_async(this, ssb->get(), 
                        m_timeout_ms, Table::SCANNER_FLAG_IGNORE_INDEX);

      m_readahead_count++;
      delete ssb;
      m_sspecs_cond.notify_one();

      std::lock_guard<std::mutex> lock(m_scanner_mutex);
      m_scanners.push_back(s);
    }

    void track_predicates(ScanCellsPtr &scancells) {
      // no results from the primary table, or LIMIT/CELL_LIMIT exceeded? 
      // then return immediately
      if ((scancells->get_eos() && scancells->empty()) || m_limits_reached) {
        sspecs_clear();
        m_eos = true;
        return;
      }

      // count cells and rows; skip CELL_OFFSET/OFFSET cells/rows and reduce
      // the results to CELL_LIMIT/LIMIT cells/rows
      ScanCellsPtr scp = std::make_shared<ScanCells>();
      Cells cells;
      scancells->get(cells);
      const char *last = m_last_rowkey_tracking.size() 
                       ? m_last_rowkey_tracking.c_str() 
                       : "";
      bool skip_row = false;
      for (auto &cell : cells) {
        bool new_row = false;
        if (strcmp(last, cell.row_key)) {
          new_row = true;
          skip_row = false;
          last = cell.row_key;
          if (m_cell_limit_per_family)
            m_cell_count = 0;
          // adjust row offset
          if (m_row_offset) {
            m_row_offset--;
            skip_row = true;
            continue;
          }
        }
        else if (skip_row)
          continue;

        // check cell offset
        if (m_cell_offset) {
          m_cell_offset--;
          continue;
        }
        // check row offset
        if (m_row_offset)
          continue;
        // check cell limit
        if (m_cell_limit && m_cell_count >= m_cell_limit) {
          m_limits_reached = true;
          break;
        }
        // check row limit
        if (m_row_limit && new_row && m_row_count >= m_row_limit) {
          m_limits_reached = true;
          break;
        }
        // check cell limit per family
        if (!m_cell_limit_per_family || m_cell_count < m_cell_limit_per_family){
          // cell pointers will go out of scope, therefore "own" is true
          scp->add(cell, true);
        }

        m_cell_count++;
        if (new_row)
          m_row_count++;
      }

      // store the contents of "last" before it goes out of scope
      m_last_rowkey_tracking = last;

      // send the results to the original callback
      if (scp->size())
        m_original_cb->scan_ok(m_primary_scanner, scp);
    }

    bool row_intervals_match(const RowIntervals &rivec, const char *row) {
      for (const auto &ri : rivec) {
        if (ri.start && ri.start[0]) {
          if (ri.start_inclusive) {
            if (strcmp(row, ri.start)<0)
              continue;
          }
          else {
            if (strcmp(row, ri.start)<=0)
              continue;
          }
        }
        if (ri.end && ri.end[0]) {
          if (ri.end_inclusive) {
            if (strcmp(row, ri.end)>0)
              continue;
          }
          else {
            if (strcmp(row, ri.end)>=0)
              continue;
          }
        }
        return true;
      } 
      return false;
    }

    bool cell_intervals_match(const CellIntervals &civec, const char *row,
            const char *column) {
      for (const auto &ci : civec) {
        if (ci.start_row && ci.start_row[0]) {
          int s=strcmp(row, ci.start_row);
          if (s>0)
            return true;
          if (s<0)
            continue;
        }
        if (ci.start_column && ci.start_column[0]) {
          if (ci.start_inclusive) {
            if (strcmp(column, ci.start_column)<0)
              continue;
          }
          else {
            if (strcmp(column, ci.start_column)<=0)
              continue;
          }
        }
        if (ci.end_row && ci.end_row[0]) {
          int s=strcmp(row, ci.end_row);
          if (s<0)
            return true;
          if (s>0)
            continue;
        }
        if (ci.end_column && ci.end_column[0]) {
          if (ci.end_inclusive) {
            if (strcmp(column, ci.end_column)>0)
              continue;
          }
          else {
            if (strcmp(column, ci.end_column)>=0)
              continue;
          }
        }
        return true;
      } 
      return false;
    }

    typedef std::map<KeySpec, uint32_t> CkeyMap;
    typedef std::map<String, String> CstrMap;

    // a weak pointer to the primary scanner
    TableScannerAsync* m_primary_scanner {};

    // a pointer to the primary table
    Table *m_primary_table {};

    // the original scan spec for the primary table
    ScanSpecBuilder m_primary_spec;

    // Vector of first-pass cell predicates
    std::vector<CellPredicate> m_cell_predicates;

    // the original callback object specified by the user
    ResultCallback *m_original_cb {};

    // the original timeout value specified by the user
    uint32_t m_timeout_ms {};

    // a list of all scanners that are created in this object
    std::vector<TableScannerAsync *> m_scanners;

    // a mutex for m_scanners
    std::mutex m_scanner_mutex;

    // a deque of ScanSpecs, needed for readahead in the primary table
    std::deque<ScanSpecBuilder *> m_sspecs;

    // a condition to wait if the sspecs-queue is too full
    std::condition_variable m_sspecs_cond;

    // a mapping from column id to column name
    std::map<uint32_t, String> m_column_map;

    // the temporary table; can be NULL
    TablePtr m_tmp_table;

    // a mutator for the temporary table
    std::unique_ptr<TableMutatorAsync> m_mutator;

    // limit and offset values from the original ScanSpec
    int m_row_limit {};
    int m_cell_limit {};
    int m_cell_count {};
    int m_row_offset {};
    int m_cell_offset {};
    int m_row_count {};
    int m_cell_limit_per_family {};

    // we reached eos - no need to continue scanning
    bool m_eos {};

    // track limits and offsets
    bool m_track_limits {};

    // limits were reached, all following keys are discarded
    bool m_limits_reached {};

    // a mutex 
    std::mutex m_mutex;

    // counting the read-ahead scans
    int m_readahead_count {};

    // temporary storage to persist pointer data before it goes out of scope
    std::string m_last_rowkey_verify;
    
    // temporary storage to persist pointer data before it goes out of scope
    std::string m_last_rowkey_tracking;

    // Carry-over matching bits for last key
    uint32_t m_cur_matching {};

    // Bit-pattern for all matching predicates
    uint32_t m_all_matching {};

    // true if this index is a qualifier index
    bool m_qualifier_scan {};

    // true if the row intervals have been applied to the index scan
    bool m_row_intervals_applied {};

    // buffer for accumulating keys from the index
    CkeyMap m_tmp_keys;

    // buffer for accumulating keys from the index
    FlyweightString m_strings;

    // accumulator; if > TMP_CUTOFF then store all index results in a
    // temporary table
    size_t m_tmp_cutoff {};

    // keep track whether we called final_decrement() 
    bool m_final_decrement {};

    // number of outstanding scanners (this is more precise than m_outstanding)
    std::atomic<int> m_outstanding_scanners {0};

    // shutting down this scanner?
    bool m_shutdown {};
  };

  inline bool operator<(const KeySpec &lhs, const KeySpec &rhs) {
    size_t len1 = strlen((const char *)lhs.row); 
    size_t len2 = strlen((const char *)rhs.row); 
    int cmp = memcmp(lhs.row, rhs.row, std::min(len1, len2));
    if (cmp > 0) 
      return false;
    if (cmp < 0)
      return true;
    if (len1 < len2)
      return true;
    return false;
  }
}

#endif // Hypertable_Lib_IndexScannerCallback_h
