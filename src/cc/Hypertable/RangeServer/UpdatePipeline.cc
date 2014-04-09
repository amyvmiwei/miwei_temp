/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/// @file
/// Definitions for UpdatePipeline.
/// This file contains type definitions for UpdatePipeline, a three-staged,
/// multithreaded update pipeline.

#include <Common/Compat.h>
#include "UpdatePipeline.h"

#include <Hypertable/RangeServer/Global.h>
#include <Hypertable/RangeServer/ResponseCallbackUpdate.h>
#include <Hypertable/RangeServer/UpdateContext.h>
#include <Hypertable/RangeServer/UpdateRecRange.h>
#include <Hypertable/RangeServer/UpdateRecTable.h>

#include <Hypertable/Lib/ClusterId.h>

#include <Common/DynamicBuffer.h>
#include <Common/FailureInducer.h>
#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace std;

UpdatePipeline::UpdatePipeline(ContextPtr &context, QueryCachePtr &query_cache,
                               TimerHandlerPtr &timer_handler) :
  m_context(context), m_query_cache(query_cache),
  m_timer_handler(timer_handler) {
  m_update_coalesce_limit = m_context->props->get_i64("Hypertable.RangeServer.UpdateCoalesceLimit");
  m_maintenance_pause_interval = m_context->props->get_i32("Hypertable.RangeServer.Testing.MaintenanceNeeded.PauseInterval");
  m_update_delay = m_context->props->get_i32("Hypertable.RangeServer.UpdateDelay", 0);
  m_max_clock_skew = m_context->props->get_i32("Hypertable.RangeServer.ClockSkew.Max");
  m_threads.reserve(3);
  m_threads.push_back( thread(&UpdatePipeline::qualify_and_transform, this) );
  m_threads.push_back( thread(&UpdatePipeline::commit, this) );
  m_threads.push_back( thread(&UpdatePipeline::add_and_respond, this) );
}

void UpdatePipeline::add(UpdateContext *uc) {
  ScopedLock lock(m_qualify_queue_mutex);
  m_qualify_queue.push_back(uc);
  m_qualify_queue_cond.notify_all();
}



void UpdatePipeline::shutdown() {
  m_shutdown = true;
  m_qualify_queue_cond.notify_all();
  m_commit_queue_cond.notify_all();
  m_response_queue_cond.notify_all();
  for (std::thread &t : m_threads)
    t.join();
}


void UpdatePipeline::qualify_and_transform() {
  UpdateContext *uc;
  SerializedKey key;
  const uint8_t *mod, *mod_end;
  const char *row;
  String start_row, end_row;
  UpdateRecRangeList *rulist;
  int error = Error::OK;
  int64_t latest_range_revision;
  RangeTransferInfo transfer_info;
  bool transfer_pending;
  DynamicBuffer *cur_bufp;
  DynamicBuffer *transfer_bufp;
  uint32_t go_buf_reset_offset;
  uint32_t root_buf_reset_offset;
  CommitLogPtr transfer_log;
  UpdateRecRange range_update;
  RangePtr range;
  Mutex &mutex = m_qualify_queue_mutex;
  boost::condition &cond = m_qualify_queue_cond;
  std::list<UpdateContext *> &queue = m_qualify_queue;

  while (true) {

    {
      ScopedLock lock(mutex);
      while (queue.empty() && !m_shutdown)
        cond.wait(lock);
      if (m_shutdown)
        return;
      uc = queue.front();
      queue.pop_front();
    }

    rulist = 0;
    transfer_bufp = 0;
    go_buf_reset_offset = 0;
    root_buf_reset_offset = 0;

    // This probably shouldn't happen for group commit, but since
    // it's only for testing purposes, we'll leave it here
    if (m_update_delay)
      poll(0, 0, m_update_delay);

    // Global commit log is only available after local recovery
    uc->auto_revision = Hypertable::get_ts64();

    // TODO: Sanity check mod data (checksum validation)

    // hack to workaround xen timestamp issue
    if (uc->auto_revision < m_last_revision)
      uc->auto_revision = m_last_revision;

    foreach_ht (UpdateRecTable *table_update, uc->updates) {

      HT_DEBUG_OUT <<"Update: "<< table_update->id << HT_END;

      if (!table_update->id.is_system() && m_context->server_state->readonly()) {
        table_update->error = Error::RANGESERVER_SERVER_IN_READONLY_MODE;
        continue;
      }

      try {
        if (!m_context->live_map->lookup(table_update->id.id, table_update->table_info)) {
          table_update->error = Error::TABLE_NOT_FOUND;
          table_update->error_msg = table_update->id.id;
          continue;
        }
      }
      catch (Exception &e) {
        table_update->error = e.code();
        table_update->error_msg = e.what();
        continue;
      }

      // verify schema
      if (table_update->table_info->get_schema()->get_generation() !=
          table_update->id.generation) {
        table_update->error = Error::RANGESERVER_GENERATION_MISMATCH;
        table_update->error_msg =
          format("Update schema generation mismatch for table %s (received %u != %u)",
                 table_update->id.id, table_update->id.generation,
                 table_update->table_info->get_schema()->get_generation());
        continue;
      }

      // Pre-allocate the go_buf - each key could expand by 8 or 9 bytes,
      // if auto-assigned (8 for the ts or rev and maybe 1 for possible
      // increase in vint length)
      table_update->go_buf.reserve(table_update->id.encoded_length() +
                                   table_update->total_buffer_size +
                                   (table_update->total_count * 9));
      table_update->id.encode(&table_update->go_buf.ptr);
      table_update->go_buf.set_mark();

      foreach_ht (UpdateRequest *request, table_update->requests) {
        uc->total_updates++;

        mod_end = request->buffer.base + request->buffer.size;
        mod = request->buffer.base;

        go_buf_reset_offset = table_update->go_buf.fill();
        root_buf_reset_offset = uc->root_buf.fill();

        memset(&uc->send_back, 0, sizeof(uc->send_back));

        while (mod < mod_end) {
          key.ptr = mod;
          row = key.row();

          // error inducer for tests/integration/fail-index-mutator
          if (HT_FAILURE_SIGNALLED("fail-index-mutator-0")) {
            if (!strcmp(row, "1,+9RfmqoH62hPVvDTh6EC4zpTNfzNr8\t\t01918")) {
              uc->send_back.count++;
              uc->send_back.error = Error::INDUCED_FAILURE;
              uc->send_back.offset = mod - request->buffer.base;
              uc->send_back.len = strlen(row);
              request->send_back_vector.push_back(uc->send_back);
              memset(&uc->send_back, 0, sizeof(uc->send_back));
              key.next(); // skip key
              key.next(); // skip value;
              mod = key.ptr;
              continue;
            }
          }

          // If the row key starts with '\0' then the buffer is probably
          // corrupt, so mark the remaing key/value pairs as bad
          if (*row == 0) {
            uc->send_back.error = Error::BAD_KEY;
            uc->send_back.count = request->count;  // fix me !!!!
            uc->send_back.offset = mod - request->buffer.base;
            uc->send_back.len = mod_end - mod;
            request->send_back_vector.push_back(uc->send_back);
            memset(&uc->send_back, 0, sizeof(uc->send_back));
            mod = mod_end;
            continue;
          }

          // Look for containing range, add to stop mods if not found
          if (!table_update->table_info->find_containing_range(row, range,
                                                          start_row, end_row) ||
              range->get_relinquish()) {
            if (uc->send_back.error != Error::RANGESERVER_OUT_OF_RANGE
                && uc->send_back.count > 0) {
              uc->send_back.len = (mod - request->buffer.base) - uc->send_back.offset;
              request->send_back_vector.push_back(uc->send_back);
              memset(&uc->send_back, 0, sizeof(uc->send_back));
            }
            if (uc->send_back.count == 0) {
              uc->send_back.error = Error::RANGESERVER_OUT_OF_RANGE;
              uc->send_back.offset = mod - request->buffer.base;
            }
            key.next(); // skip key
            key.next(); // skip value;
            mod = key.ptr;
            uc->send_back.count++;
            continue;
          }

          if ((rulist = table_update->range_map[range.get()]) == 0) {
            rulist = new UpdateRecRangeList();
            rulist->range = range;
            table_update->range_map[range.get()] = rulist;
          }

          // See if range has some other error preventing it from receiving updates
          if ((error = rulist->range->get_error()) != Error::OK) {
            if (uc->send_back.error != error && uc->send_back.count > 0) {
              uc->send_back.len = (mod - request->buffer.base) - uc->send_back.offset;
              request->send_back_vector.push_back(uc->send_back);
              memset(&uc->send_back, 0, sizeof(uc->send_back));
            }
            if (uc->send_back.count == 0) {
              uc->send_back.error = error;
              uc->send_back.offset = mod - request->buffer.base;
            }
            key.next(); // skip key
            key.next(); // skip value;
            mod = key.ptr;
            uc->send_back.count++;
            continue;
          }

          if (uc->send_back.count > 0) {
            uc->send_back.len = (mod - request->buffer.base) - uc->send_back.offset;
            request->send_back_vector.push_back(uc->send_back);
            memset(&uc->send_back, 0, sizeof(uc->send_back));
          }

          /*
           *  Increment update count on range
           *  (block if maintenance in progress)
           */
          if (!rulist->range_blocked) {
            if (!rulist->range->increment_update_counter()) {
              uc->send_back.error = Error::RANGESERVER_RANGE_NOT_FOUND;
              uc->send_back.offset = mod - request->buffer.base;
              uc->send_back.count++;
              key.next(); // skip key
              key.next(); // skip value;
              mod = key.ptr;
              continue;
            }
            rulist->range_blocked = true;
          }

          String range_start_row, range_end_row;
          rulist->range->get_boundary_rows(range_start_row, range_end_row);

          // Make sure range didn't just shrink
          if (range_start_row != start_row || range_end_row != end_row) {
            rulist->range->decrement_update_counter();
            table_update->range_map.erase(rulist->range.get());
            delete rulist;
            continue;
          }

          /** Fetch range transfer information **/
          {
            bool wait_for_maintenance;
            transfer_pending = rulist->range->get_transfer_info(transfer_info, transfer_log,
                                                                &latest_range_revision, wait_for_maintenance);
          }

          if (rulist->transfer_log.get() == 0)
            rulist->transfer_log = transfer_log;

          HT_ASSERT(rulist->transfer_log.get() == transfer_log.get());

          bool in_transferring_region = false;

          // Check for clock skew
          {
            ByteString tmp_key;
            const uint8_t *tmp;
            int64_t difference, tmp_timestamp;
            tmp_key.ptr = key.ptr;
            tmp_key.decode_length(&tmp);
            if ((*tmp & Key::HAVE_REVISION) == 0) {
              if (latest_range_revision > TIMESTAMP_MIN
                  && uc->auto_revision < latest_range_revision) {
                tmp_timestamp = Hypertable::get_ts64();
                if (tmp_timestamp > uc->auto_revision)
                  uc->auto_revision = tmp_timestamp;
                if (uc->auto_revision < latest_range_revision) {
                  difference = (int32_t)((latest_range_revision - uc->auto_revision)
                                         / 1000LL);
                  if (difference > m_max_clock_skew && !Global::ignore_clock_skew_errors) {
                    request->error = Error::RANGESERVER_CLOCK_SKEW;
                    HT_ERRORF("Clock skew of %lld microseconds exceeds maximum "
                              "(%lld) range=%s", (Lld)difference,
                              (Lld)m_max_clock_skew,
                              rulist->range->get_name().c_str());
                    uc->send_back.count = 0;
                    request->send_back_vector.clear();
                    break;
                  }
                }
              }
            }
          }

          if (transfer_pending) {
            transfer_bufp = &rulist->transfer_buf;
            if (transfer_bufp->empty()) {
              transfer_bufp->reserve(table_update->id.encoded_length());
              table_update->id.encode(&transfer_bufp->ptr);
              transfer_bufp->set_mark();
            }
            rulist->transfer_buf_reset_offset = rulist->transfer_buf.fill();
          }
          else {
            transfer_bufp = 0;
            rulist->transfer_buf_reset_offset = 0;
          }

          if (rulist->range->is_root()) {
            if (uc->root_buf.empty()) {
              uc->root_buf.reserve(table_update->id.encoded_length());
              table_update->id.encode(&uc->root_buf.ptr);
              uc->root_buf.set_mark();
              root_buf_reset_offset = uc->root_buf.fill();
            }
            cur_bufp = &uc->root_buf;
          }
          else
            cur_bufp = &table_update->go_buf;

          rulist->last_request = request;

          range_update.bufp = cur_bufp;
          range_update.offset = cur_bufp->fill();

          while (mod < mod_end &&
                 (end_row == "" || (strcmp(row, end_row.c_str()) <= 0))) {

            if (transfer_pending) {

              if (transfer_info.transferring(row)) {
                if (!in_transferring_region) {
                  range_update.len = cur_bufp->fill() - range_update.offset;
                  rulist->add_update(request, range_update);
                  cur_bufp = transfer_bufp;
                  range_update.bufp = cur_bufp;
                  range_update.offset = cur_bufp->fill();
                  in_transferring_region = true;
                }
                table_update->transfer_count++;
              }
              else {
                if (in_transferring_region) {
                  range_update.len = cur_bufp->fill() - range_update.offset;
                  rulist->add_update(request, range_update);
                  cur_bufp = &table_update->go_buf;
                  range_update.bufp = cur_bufp;
                  range_update.offset = cur_bufp->fill();
                  in_transferring_region = false;
                }
              }
            }

            try {
              SchemaPtr schema = table_update->table_info->get_schema();
              uint8_t family=*(key.ptr+1+strlen((const char *)key.ptr+1)+1);
              Schema::ColumnFamily *cf = schema->get_column_family(family);

              // reset auto_revision if it's gotten behind
              if (uc->auto_revision < latest_range_revision) {
                uc->auto_revision = Hypertable::get_ts64();
                if (uc->auto_revision < latest_range_revision) {
                  HT_THROWF(Error::RANGESERVER_REVISION_ORDER_ERROR,
                          "Auto revision (%lld) is less than latest range "
                          "revision (%lld) for range %s",
                          (Lld)uc->auto_revision, (Lld)latest_range_revision,
                          rulist->range->get_name().c_str());
                }
              }

              // This will transform keys that need to be assigned a
              // timestamp and/or revision number by re-writing the key
              // with the added timestamp and/or revision tacked on to the end
              transform_key(key, cur_bufp, ++uc->auto_revision,
                      &m_last_revision, cf ? cf->time_order_desc : false);

              // Validate revision number
              if (m_last_revision < latest_range_revision) {
                if (m_last_revision != uc->auto_revision) {
                  HT_THROWF(Error::RANGESERVER_REVISION_ORDER_ERROR,
                          "Supplied revision (%lld) is less than most recently "
                          "seen revision (%lld) for range %s",
                          (Lld)m_last_revision, (Lld)latest_range_revision,
                          rulist->range->get_name().c_str());
                }
              }
            }
            catch (Exception &e) {
              HT_ERRORF("%s - %s", e.what(), Error::get_text(e.code()));
              request->error = e.code();
              break;
            }

            // Now copy the value (with sanity check)
            mod = key.ptr;
            key.next(); // skip value
            HT_ASSERT(key.ptr <= mod_end);
            cur_bufp->add(mod, key.ptr-mod);
            mod = key.ptr;

            table_update->total_added++;

            if (mod < mod_end)
              row = key.row();
          }

          if (request->error == Error::OK) {

            range_update.len = cur_bufp->fill() - range_update.offset;
            rulist->add_update(request, range_update);

            // if there were transferring updates, record the latest revision
            if (transfer_pending && rulist->transfer_buf_reset_offset < rulist->transfer_buf.fill()) {
              if (rulist->latest_transfer_revision < m_last_revision)
                rulist->latest_transfer_revision = m_last_revision;
            }
          }
          else {
            /*
             * If we drop into here, this means that the request is
             * being aborted, so reset all of the UpdateRecRangeLists,
             * reset the go_buf and the root_buf
             */
            for (std::unordered_map<Range *, UpdateRecRangeList *>::iterator iter = table_update->range_map.begin();
                 iter != table_update->range_map.end(); ++iter)
              (*iter).second->reset_updates(request);
            table_update->go_buf.ptr = table_update->go_buf.base + go_buf_reset_offset;
            if (root_buf_reset_offset)
              uc->root_buf.ptr = uc->root_buf.base + root_buf_reset_offset;
            uc->send_back.count = 0;
            mod = mod_end;
          }
          range_update.bufp = 0;
        }

        transfer_log = 0;

        if (uc->send_back.count > 0) {
          uc->send_back.len = (mod - request->buffer.base) - uc->send_back.offset;
          request->send_back_vector.push_back(uc->send_back);
          memset(&uc->send_back, 0, sizeof(uc->send_back));
        }
      }

      HT_DEBUGF("Added %d (%d transferring) updates to '%s'",
                table_update->total_added, table_update->transfer_count,
                table_update->id.id);
      if (!table_update->id.is_metadata())
        uc->total_added += table_update->total_added;
    }

    uc->last_revision = m_last_revision;

    // Enqueue update
    {
      ScopedLock lock(m_commit_queue_mutex);
      m_commit_queue.push_back(uc);
      m_commit_queue_cond.notify_all();
      m_commit_queue_count++;
    }
  }
}

void UpdatePipeline::commit() {
  UpdateContext *uc;
  SerializedKey key;
  std::list<UpdateContext *> coalesce_queue;
  uint64_t coalesce_amount = 0;
  int error = Error::OK;
  uint32_t committed_transfer_data;
  bool user_log_needs_syncing;

  while (true) {

    // Dequeue next update
    {
      ScopedLock lock(m_commit_queue_mutex);
      while (m_commit_queue.empty() && !m_shutdown)
        m_commit_queue_cond.wait(lock);
      if (m_shutdown)
        return;
      uc = m_commit_queue.front();
      m_commit_queue.pop_front();
      m_commit_queue_count--;
    }

    committed_transfer_data = 0;
    user_log_needs_syncing = false;

    /**
     * Commit ROOT mutations
     */
    if (uc->root_buf.ptr > uc->root_buf.mark) {
      if ((error = Global::root_log->write(ClusterId::get(), uc->root_buf, uc->last_revision)) != Error::OK) {
        HT_FATALF("Problem writing %d bytes to ROOT commit log - %s",
                  (int)uc->root_buf.fill(), Error::get_text(error));
      }
    }

    foreach_ht (UpdateRecTable *table_update, uc->updates) {

      coalesce_amount += table_update->total_buffer_size;

      // Iterate through all of the ranges, committing any transferring updates
      for (std::unordered_map<Range *, UpdateRecRangeList *>::iterator iter = table_update->range_map.begin(); iter != table_update->range_map.end(); ++iter) {
        if ((*iter).second->transfer_buf.ptr > (*iter).second->transfer_buf.mark) {
          committed_transfer_data += (*iter).second->transfer_buf.ptr - (*iter).second->transfer_buf.mark;
          if ((error = (*iter).second->transfer_log->write(ClusterId::get(), (*iter).second->transfer_buf, (*iter).second->latest_transfer_revision)) != Error::OK) {
            table_update->error = error;
            table_update->error_msg = format("Problem writing %d bytes to transfer log",
                                             (int)(*iter).second->transfer_buf.fill());
            HT_ERRORF("%s - %s", table_update->error_msg.c_str(), Error::get_text(error));
            break;
          }
        }
      }

      if (table_update->error != Error::OK)
        continue;

      /**
       * Commit valid (go) mutations
       */
      if (table_update->go_buf.ptr > table_update->go_buf.mark) {
        CommitLog *log = 0;

        bool sync = false;
        if (table_update->id.is_user()) {
          log = Global::user_log;
          if ((table_update->flags & RangeServerProtocol::UPDATE_FLAG_NO_LOG_SYNC) == 0)
            user_log_needs_syncing = true;
        }
        else if (table_update->id.is_metadata()) {
          sync = true;
          log = Global::metadata_log;
        }
        else {
          HT_ASSERT(table_update->id.is_system());
          sync = true;
          log = Global::system_log;
        }

        if ((error = log->write(ClusterId::get(), table_update->go_buf, uc->last_revision, sync)) != Error::OK) {
          table_update->error_msg = format("Problem writing %d bytes to commit log (%s) - %s",
                                           (int)table_update->go_buf.fill(),
                                           log->get_log_dir().c_str(),
                                           Error::get_text(error));
          HT_ERRORF("%s", table_update->error_msg.c_str());
          table_update->error = error;
          continue;
        }
      }
      else if (table_update->sync)
        user_log_needs_syncing = true;

    }

    bool do_sync = false;
    if (user_log_needs_syncing) {
      if (m_commit_queue_count > 0 && coalesce_amount < m_update_coalesce_limit) {
        coalesce_queue.push_back(uc);
        continue;
      }
      do_sync = true;
    }
    else if (!coalesce_queue.empty())
      do_sync = true;

    // Now sync the USER commit log if needed
    if (do_sync) {
      size_t retry_count = 0;
      uc->total_syncs++;
      while ((error = Global::user_log->sync()) != Error::OK) {
        HT_ERRORF("Problem sync'ing user log fragment (%s) - %s",
                  Global::user_log->get_current_fragment_file().c_str(),
                  Error::get_text(error));
        if (++retry_count == 6)
          break;
        poll(0, 0, 10000);
      }
    }

    // Enqueue update
    {
      ScopedLock lock(m_response_queue_mutex);
      coalesce_queue.push_back(uc);
      while (!coalesce_queue.empty()) {
        uc = coalesce_queue.front();
        coalesce_queue.pop_front();
        m_response_queue.push_back(uc);
      }
      coalesce_amount = 0;
      m_response_queue_cond.notify_all();
    }
  }
}

void UpdatePipeline::add_and_respond() {
  UpdateContext *uc;
  SerializedKey key;
  int error = Error::OK;

  while (true) {

    // Dequeue next update
    {
      ScopedLock lock(m_response_queue_mutex);
      while (m_response_queue.empty() && !m_shutdown)
        m_response_queue_cond.wait(lock);
      if (m_shutdown)
        return;
      uc = m_response_queue.front();
      m_response_queue.pop_front();
    }

    /**
     *  Insert updates into Ranges
     */
    foreach_ht (UpdateRecTable *table_update, uc->updates) {

      // Iterate through all of the ranges, inserting updates
      for (std::unordered_map<Range *, UpdateRecRangeList *>::iterator iter = table_update->range_map.begin(); iter != table_update->range_map.end(); ++iter) {
        ByteString value;
        Key key_comps;

        foreach_ht (UpdateRecRange &update, (*iter).second->updates) {
          Range *rangep = (*iter).first;
          Locker<Range> lock(*rangep);
          uint8_t *ptr = update.bufp->base + update.offset;
          uint8_t *end = ptr + update.len;

          if (!table_update->id.is_metadata())
            uc->total_bytes_added += update.len;

          rangep->add_bytes_written( update.len );
          const char *last_row = "";
          uint64_t count = 0;
          while (ptr < end) {
            key.ptr = ptr;
            key_comps.load(key);
            count++;
            if (key_comps.column_family_code == 0 && key_comps.flag != FLAG_DELETE_ROW) {
              HT_ERRORF("Skipping bad key - column family not specified in non-delete row update on %s row=%s",
                        table_update->id.id, key_comps.row);
            }
            ptr += key_comps.length;
            value.ptr = ptr;
            ptr += value.length();
            rangep->add(key_comps, value);
            // invalidate
            if (m_query_cache && strcmp(last_row, key_comps.row))
              m_query_cache->invalidate(table_update->id.id, key_comps.row);
            last_row = key_comps.row;
          }
          rangep->add_cells_written(count);
        }
      }
    }

    /**
     * Decrement usage counters for all referenced ranges
     */
    foreach_ht (UpdateRecTable *table_update, uc->updates) {
      for (std::unordered_map<Range *, UpdateRecRangeList *>::iterator iter = table_update->range_map.begin(); iter != table_update->range_map.end(); ++iter) {
        if ((*iter).second->range_blocked)
          (*iter).first->decrement_update_counter();
      }
    }

    /**
     * wait for these ranges to complete maintenance
     */
    bool maintenance_needed = false;
    foreach_ht (UpdateRecTable *table_update, uc->updates) {

      /**
       * If any of the newly updated ranges needs maintenance,
       * schedule immediately
       */
      for (std::unordered_map<Range *, UpdateRecRangeList *>::iterator iter = table_update->range_map.begin(); iter != table_update->range_map.end(); ++iter) {
        if ((*iter).first->need_maintenance() &&
            !Global::maintenance_queue->contains((*iter).first)) {
          maintenance_needed = true;
          HT_MAYBE_FAIL_X("metadata-update-and-respond", (*iter).first->is_metadata());
          if (m_timer_handler)
            m_timer_handler->schedule_immediate_maintenance();
          break;
        }
      }

      foreach_ht (UpdateRequest *request, table_update->requests) {
        ResponseCallbackUpdate cb(m_context->comm, request->event);

        if (table_update->error != Error::OK) {
          if ((error = cb.error(table_update->error, table_update->error_msg)) != Error::OK)
            HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
          continue;
        }

        if (request->error == Error::OK) {
          /**
           * Send back response
           */
          if (!request->send_back_vector.empty()) {
            StaticBuffer ext(new uint8_t [request->send_back_vector.size() * 16],
                             request->send_back_vector.size() * 16);
            uint8_t *ptr = ext.base;
            for (size_t i=0; i<request->send_back_vector.size(); i++) {
              Serialization::encode_i32(&ptr, request->send_back_vector[i].error);
              Serialization::encode_i32(&ptr, request->send_back_vector[i].count);
              Serialization::encode_i32(&ptr, request->send_back_vector[i].offset);
              Serialization::encode_i32(&ptr, request->send_back_vector[i].len);
              /*
                HT_INFOF("Sending back error %x, count %d, offset %d, len %d, table id %s",
                request->send_back_vector[i].error, request->send_back_vector[i].count,
                request->send_back_vector[i].offset, request->send_back_vector[i].len,
                table_update->id.id);
              */
            }
            if ((error = cb.response(ext)) != Error::OK)
              HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
          }
          else {
            if ((error = cb.response_ok()) != Error::OK)
              HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
          }
        }
        else {
          if ((error = cb.error(request->error, "")) != Error::OK)
            HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
        }
      }

    }

    {
      Locker<LoadStatistics> lock(*Global::load_statistics);
      Global::load_statistics->add_update_data(uc->total_updates, uc->total_added, uc->total_bytes_added, uc->total_syncs);
    }

    delete uc;

    // For testing
    if (m_maintenance_pause_interval > 0 && maintenance_needed)
      poll(0, 0, m_maintenance_pause_interval);

  }
}


void
UpdatePipeline::transform_key(ByteString &bskey, DynamicBuffer *dest_bufp,
                              int64_t auto_revision, int64_t *revisionp,
                              bool timeorder_desc) {
  size_t len;
  const uint8_t *ptr;

  len = bskey.decode_length(&ptr);

  HT_ASSERT(*ptr == Key::AUTO_TIMESTAMP || *ptr == Key::HAVE_TIMESTAMP);

  // if TIME_ORDER DESC was set for this column then we store the timestamps
  // NOT in 1-complements!
  if (timeorder_desc) {
    // if the timestamp was specified by the user: unpack it and pack it
    // again w/o 1-complement
    if (*ptr == Key::HAVE_TIMESTAMP) {
      uint8_t *p=(uint8_t *)ptr+len-8;
      int64_t ts=Key::decode_ts64((const uint8_t **)&p);
      p=(uint8_t *)ptr+len-8;
      Key::encode_ts64((uint8_t **)&p, ts, false);
    }
  }

  dest_bufp->ensure((ptr-bskey.ptr) + len + 9);
  Serialization::encode_vi32(&dest_bufp->ptr, len+8);
  memcpy(dest_bufp->ptr, ptr, len);
  if (*ptr == Key::AUTO_TIMESTAMP)
    *dest_bufp->ptr = Key::HAVE_REVISION
        | Key::HAVE_TIMESTAMP | Key::REV_IS_TS;
  else
    *dest_bufp->ptr = Key::HAVE_REVISION
        | Key::HAVE_TIMESTAMP;

  // if TIME_ORDER DESC then store a flag in the key
  if (timeorder_desc)
    *dest_bufp->ptr |= Key::TS_CHRONOLOGICAL;

  dest_bufp->ptr += len;
  Key::encode_ts64(&dest_bufp->ptr, auto_revision,
          timeorder_desc ? false : true);
  *revisionp = auto_revision;
  bskey.ptr = ptr + len;
}
