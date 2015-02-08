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
 * Definitions for Client.
 * This file contains definitions for Client, a client interface
 * class to the RangeServer.
 */

#include <Common/Compat.h>

#include "Client.h"
#include "Protocol.h"
#include "Request/Parameters/AcknowledgeLoad.h"
#include "Request/Parameters/CommitLogSync.h"
#include "Request/Parameters/Compact.h"
#include "Request/Parameters/CreateScanner.h"
#include "Request/Parameters/DestroyScanner.h"
#include "Request/Parameters/DropRange.h"
#include "Request/Parameters/DropTable.h"
#include "Request/Parameters/Dump.h"
#include "Request/Parameters/DumpPseudoTable.h"
#include "Request/Parameters/FetchScanblock.h"
#include "Request/Parameters/GetStatistics.h"
#include "Request/Parameters/Heapcheck.h"
#include "Request/Parameters/LoadRange.h"
#include "Request/Parameters/PhantomCommitRanges.h"
#include "Request/Parameters/PhantomLoad.h"
#include "Request/Parameters/PhantomPrepareRanges.h"
#include "Request/Parameters/PhantomUpdate.h"
#include "Request/Parameters/RelinquishRange.h"
#include "Request/Parameters/ReplayFragments.h"
#include "Request/Parameters/SetState.h"
#include "Request/Parameters/TableMaintenanceDisable.h"
#include "Request/Parameters/TableMaintenanceEnable.h"
#include "Request/Parameters/Update.h"
#include "Request/Parameters/UpdateSchema.h"
#include "Response/Parameters/AcknowledgeLoad.h"
#include "Response/Parameters/CreateScanner.h"
#include "Response/Parameters/GetStatistics.h"
#include "Response/Parameters/Status.h"

#include <Hypertable/Lib/ScanBlock.h>

#include <AsyncComm/DispatchHandlerSynchronizer.h>
#include <AsyncComm/Protocol.h>

#include <Common/Config.h>
#include <Common/Error.h>
#include <Common/StringExt.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

Lib::RangeServer::Client::Client(Comm *comm, int32_t timeout_ms)
  : m_comm(comm), m_default_timeout_ms(timeout_ms) {
  if (timeout_ms == 0)
    m_default_timeout_ms = get_i32("Hypertable.Request.Timeout");
}


Lib::RangeServer::Client::~Client() {
}


void Lib::RangeServer::Client::compact(const CommAddress &addr, const TableIdentifier &table,
                     const String &row, int32_t flags) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommHeader header(Protocol::COMMAND_COMPACT);
  Request::Parameters::Compact params(table, row, flags);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());

  send_message(addr, cbuf, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer compact() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}

void Lib::RangeServer::Client::compact(const CommAddress &addr, const TableIdentifier &table,
                     const String &row, int32_t flags,
                     DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_COMPACT);
  Request::Parameters::Compact params(table, row, flags);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void Lib::RangeServer::Client::compact(const CommAddress &addr, const TableIdentifier &table,
                     const String &row, int32_t flags,
                     DispatchHandler *handler, Timer &timer) {
  CommHeader header(Protocol::COMMAND_COMPACT);
  Request::Parameters::Compact params(table, row, flags);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}


void
Lib::RangeServer::Client::load_range(const CommAddress &addr,
                   const TableIdentifier &table, const RangeSpec &range_spec,
                   const RangeState &range_state, bool needs_compaction) {
  do_load_range(addr, table, range_spec, range_state, needs_compaction,
                m_default_timeout_ms);
}

void
Lib::RangeServer::Client::load_range(const CommAddress &addr,
                   const TableIdentifier &table, const RangeSpec &range_spec,
                   const RangeState &range_state, bool needs_compaction,
    Timer &timer) {
  do_load_range(addr, table, range_spec, range_state, needs_compaction,
                timer.remaining());
}

void
Lib::RangeServer::Client::do_load_range(const CommAddress &addr,
                      const TableIdentifier &table, const RangeSpec &range_spec,
                      const RangeState &range_state, bool needs_compaction,
                      int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommHeader header(Protocol::COMMAND_LOAD_RANGE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::LoadRange params(table, range_spec, range_state, needs_compaction);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());

  send_message(addr, cbuf, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
               String("RangeServer load_range() failure : ")
               + Hypertable::Protocol::string_format_message(event));
}

void
Lib::RangeServer::Client::acknowledge_load(const CommAddress &addr,
                         const vector<QualifiedRangeSpec*> &ranges,
                         map<QualifiedRangeSpec, int> &response_map) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;

  // Initialize response map
  for (auto spec : ranges)
    response_map[*spec] = Error::NO_RESPONSE;

  CommHeader header(Protocol::COMMAND_ACKNOWLEDGE_LOAD);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::AcknowledgeLoad params(ranges);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());

  send_message(addr, cbuf, &sync_handler, m_default_timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer acknowledge_load() failure : ")
             + Hypertable::Protocol::string_format_message(event));

  // Decode response
  {
    const uint8_t *ptr = event->payload + 4;
    size_t remain = event->payload_len - 4;
    Response::Parameters::AcknowledgeLoad params;
    params.decode(&ptr, &remain);
    // Copy error codes into response map
    for (auto & entry : params.error_map()) {
      auto iter = response_map.find(entry.first);
      HT_ASSERT(iter != response_map.end());
      iter->second = entry.second;
    }

  }
}

void Lib::RangeServer::Client::update(const CommAddress &addr, uint64_t cluster_id, 
                    const TableIdentifier &table, int32_t count,
                    StaticBuffer &buffer, int32_t flags,
                    DispatchHandler *handler) {

  CommHeader header(Protocol::COMMAND_UPDATE);
  if (table.is_system())
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::Update params(cluster_id, table, count, flags);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length(), buffer));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void
Lib::RangeServer::Client::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_CREATE_SCANNER);
  header.flags |= CommHeader::FLAGS_BIT_PROFILE;
  if (table.is_system())
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::CreateScanner params(table, range, scan_spec);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void
Lib::RangeServer::Client::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, DispatchHandler *handler,
    Timer &timer) {
  CommHeader header(Protocol::COMMAND_CREATE_SCANNER);
  header.flags |= CommHeader::FLAGS_BIT_PROFILE;
  if (table.is_system())
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::CreateScanner params(table, range, scan_spec);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}

void
Lib::RangeServer::Client::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, ScanBlock &scan_block) {
  do_create_scanner(addr, table, range, scan_spec,
                    scan_block, m_default_timeout_ms);
}

void
Lib::RangeServer::Client::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, ScanBlock &scan_block,
    Timer &timer) {
  do_create_scanner(addr, table, range, scan_spec,
                    scan_block, timer.remaining());
}

void
Lib::RangeServer::Client::do_create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, ScanBlock &scan_block,
    int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommHeader header(Protocol::COMMAND_CREATE_SCANNER);
  header.flags |= CommHeader::FLAGS_BIT_PROFILE;
  if (table.is_system())
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::CreateScanner params(table, range, scan_spec);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());

  send_message(addr, cbuf, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer create_scanner() failure : ")
             + Hypertable::Protocol::string_format_message(event));
  else {
    HT_ASSERT(scan_block.load(event) == Error::OK);
  }
}


void
Lib::RangeServer::Client::destroy_scanner(const CommAddress &addr, int32_t scanner_id,
                        DispatchHandler *handler) {

  CommHeader header(Protocol::COMMAND_DESTROY_SCANNER);
  header.gid = scanner_id;
  Request::Parameters::DestroyScanner params(scanner_id);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void
Lib::RangeServer::Client::destroy_scanner(const CommAddress &addr, int32_t scanner_id,
                        DispatchHandler *handler, Timer &timer) {
  CommHeader header(Protocol::COMMAND_DESTROY_SCANNER);
  header.gid = scanner_id;
  Request::Parameters::DestroyScanner params(scanner_id);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}


void
Lib::RangeServer::Client::destroy_scanner(const CommAddress &addr, int32_t scanner_id) {
  do_destroy_scanner(addr, scanner_id, m_default_timeout_ms);
}

void
Lib::RangeServer::Client::destroy_scanner(const CommAddress &addr, int32_t scanner_id,
                        Timer &timer) {
  do_destroy_scanner(addr, scanner_id, timer.remaining());
}

void
Lib::RangeServer::Client::do_destroy_scanner(const CommAddress &addr, int32_t scanner_id,
                           int32_t timeout_ms) {

  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_DESTROY_SCANNER);
  header.gid = scanner_id;
  Request::Parameters::DestroyScanner params(scanner_id);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer destroy_scanner() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}

void
Lib::RangeServer::Client::fetch_scanblock(const CommAddress &addr, int32_t scanner_id,
                        DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_FETCH_SCANBLOCK);
  header.flags |= CommHeader::FLAGS_BIT_PROFILE;
  header.gid = scanner_id;
  Request::Parameters::FetchScanblock params(scanner_id);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void
Lib::RangeServer::Client::fetch_scanblock(const CommAddress &addr, int32_t scanner_id,
                        DispatchHandler *handler, Timer &timer) {
  CommHeader header(Protocol::COMMAND_FETCH_SCANBLOCK);
  header.flags |= CommHeader::FLAGS_BIT_PROFILE;
  header.gid = scanner_id;
  Request::Parameters::FetchScanblock params(scanner_id);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}


void
Lib::RangeServer::Client::fetch_scanblock(const CommAddress &addr, int32_t scanner_id,
                        ScanBlock &scan_block) {
  do_fetch_scanblock(addr, scanner_id, scan_block, m_default_timeout_ms);
}

void
Lib::RangeServer::Client::fetch_scanblock(const CommAddress &addr, int32_t scanner_id,
                        ScanBlock &scan_block, Timer &timer) {
  do_fetch_scanblock(addr, scanner_id, scan_block, timer.remaining());
}

void
Lib::RangeServer::Client::do_fetch_scanblock(const CommAddress &addr, int32_t scanner_id,
                           ScanBlock &scan_block, int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_FETCH_SCANBLOCK);
  header.flags |= CommHeader::FLAGS_BIT_PROFILE;
  header.gid = scanner_id;
  Request::Parameters::FetchScanblock params(scanner_id);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer fetch_scanblock() failure : ")
             + Hypertable::Protocol::string_format_message(event));
  else {
    HT_EXPECT(scan_block.load(event) == Error::OK,
              Error::FAILED_EXPECTATION);
  }
}


void Lib::RangeServer::Client::drop_table(const CommAddress &addr, const TableIdentifier &table,
                        DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_DROP_TABLE);
  Request::Parameters::DropTable params(table);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void Lib::RangeServer::Client::drop_table(const CommAddress &addr, const TableIdentifier &table,
                        DispatchHandler *handler, Timer &timer) {
  CommHeader header(Protocol::COMMAND_DROP_TABLE);
  Request::Parameters::DropTable params(table);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}


void Lib::RangeServer::Client::drop_table(const CommAddress &addr, const TableIdentifier &table) {
  do_drop_table(addr, table, m_default_timeout_ms);
}

void Lib::RangeServer::Client::do_drop_table(const CommAddress &addr,const TableIdentifier &table,
                           int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_DROP_TABLE);
  Request::Parameters::DropTable params(table);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer drop_table() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}

void Lib::RangeServer::Client::update_schema(const CommAddress &addr,const TableIdentifier &table,
                           const String &schema, DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_UPDATE_SCHEMA);
  if (table.is_system())
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::UpdateSchema params(table, schema);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void
Lib::RangeServer::Client::update_schema(const CommAddress &addr,
    const TableIdentifier &table, const String &schema,
    DispatchHandler *handler, Timer &timer) {
  CommHeader header(Protocol::COMMAND_UPDATE_SCHEMA);
  if (table.is_system())
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::UpdateSchema params(table, schema);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}

void Lib::RangeServer::Client::commit_log_sync(const CommAddress &addr, uint64_t cluster_id,
                             const TableIdentifier &table,
                             DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_COMMIT_LOG_SYNC);
  if (table.is_system())
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::CommitLogSync params(cluster_id, table);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void Lib::RangeServer::Client::commit_log_sync(const CommAddress &addr, uint64_t cluster_id,
                             const TableIdentifier &table,
                             DispatchHandler *handler, Timer &timer) {
  CommHeader header(Protocol::COMMAND_COMMIT_LOG_SYNC);
  if (table.is_system())
    header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::CommitLogSync params(cluster_id, table);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}

void Lib::RangeServer::Client::status(const CommAddress &addr, Status &status) {
  do_status(addr, status, m_default_timeout_ms);
}

void Lib::RangeServer::Client::status(const CommAddress &addr, Status &status, Timer &timer) {
  do_status(addr, status, timer.remaining());
}

void Lib::RangeServer::Client::do_status(const CommAddress &addr, Status &status, int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_STATUS);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBufPtr cbuf(new CommBuf(header));
  send_message(addr, cbuf, &sync_handler, timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer status() failure : ")
             + Hypertable::Protocol::string_format_message(event));

  {
    size_t remaining = event->payload_len - 4;
    const uint8_t *ptr = event->payload + 4;
    Response::Parameters::Status params;
    params.decode(&ptr, &remaining);
    status = params.status();
  }

}

void Lib::RangeServer::Client::wait_for_maintenance(const CommAddress &addr) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_WAIT_FOR_MAINTENANCE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBufPtr cbuf(new CommBuf(header));
  send_message(addr, cbuf, &sync_handler, m_default_timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer wait_for_maintenance() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}


void Lib::RangeServer::Client::shutdown(const CommAddress &addr) {
  CommHeader header(Protocol::COMMAND_SHUTDOWN);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBufPtr cbuf(new CommBuf(header));
  send_message(addr, cbuf, 0, m_default_timeout_ms);
}

void Lib::RangeServer::Client::dump(const CommAddress &addr, String &outfile, bool nokeys) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_DUMP);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::Dump params(outfile, nokeys);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, m_default_timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer dump() failure : ")
             + Hypertable::Protocol::string_format_message(event));

}

void
Lib::RangeServer::Client::dump_pseudo_table(const CommAddress &addr, const TableIdentifier &table,
                          const String &pseudo_table_name,
                          const String &outfile) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_DUMP_PSEUDO_TABLE);
  Request::Parameters::DumpPseudoTable params(table, pseudo_table_name, outfile);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, m_default_timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer dump_pseudo_table() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}

void Lib::RangeServer::Client::get_statistics(const CommAddress &addr,
                            vector<SystemVariable::Spec>&specs,
                            int64_t generation, StatsRangeServer &stats) {
  do_get_statistics(addr, specs, generation, stats, m_default_timeout_ms);
}

void Lib::RangeServer::Client::get_statistics(const CommAddress &addr,
                            vector<SystemVariable::Spec>&specs,
                            int64_t generation, StatsRangeServer &stats,
                            Timer &timer) {
  do_get_statistics(addr, specs, generation, stats, timer.remaining());
}

void Lib::RangeServer::Client::do_get_statistics(const CommAddress &addr,
                               vector<SystemVariable::Spec> &specs,
                               int64_t generation, StatsRangeServer &stats,
                               int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_GET_STATISTICS);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::GetStatistics params(specs, generation);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer get_statistics() failure : ")
             + Hypertable::Protocol::string_format_message(event));

  {
    size_t remaining = event->payload_len - 4;
    const uint8_t *ptr = event->payload + 4;
    Response::Parameters::GetStatistics params;
    params.decode(&ptr, &remaining);
    stats = params.stats();
  }
}

void Lib::RangeServer::Client::get_statistics(const CommAddress &addr,
                            vector<SystemVariable::Spec>&specs,
                            int64_t generation, DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_GET_STATISTICS);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::GetStatistics params(specs, generation);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void Lib::RangeServer::Client::get_statistics(const CommAddress &addr,
                            vector<SystemVariable::Spec>&specs,
                            int64_t generation, DispatchHandler *handler,
                            Timer &timer) {
  CommHeader header(Protocol::COMMAND_GET_STATISTICS);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::GetStatistics params(specs, generation);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}

void Lib::RangeServer::Client::drop_range(const CommAddress &addr, const TableIdentifier &table,
                        const RangeSpec &range, DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_DROP_RANGE);
  Request::Parameters::DropRange params(table, range);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void Lib::RangeServer::Client::drop_range(const CommAddress &addr, const TableIdentifier &table,
                        const RangeSpec &range, DispatchHandler *handler,
                        Timer &timer) {
  CommHeader header(Protocol::COMMAND_DROP_RANGE);
  Request::Parameters::DropRange params(table, range);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}

void Lib::RangeServer::Client::relinquish_range(const CommAddress &addr,
                              const TableIdentifier &table,
                              const RangeSpec &range) {
  do_relinquish_range(addr, table, range, m_default_timeout_ms);
}

void Lib::RangeServer::Client::relinquish_range(const CommAddress &addr,
                              const TableIdentifier &table,
                              const RangeSpec &range, Timer &timer) {
  do_relinquish_range(addr, table, range, timer.remaining());
}

void Lib::RangeServer::Client::do_relinquish_range(const CommAddress &addr,
                                 const TableIdentifier &table,
                                 const RangeSpec &range,
                                 int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_RELINQUISH_RANGE);
  Request::Parameters::RelinquishRange params(table, range);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());

  EventPtr event;
  send_message(addr, cbuf, &sync_handler, timeout_ms);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer relinquish_range() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}

void Lib::RangeServer::Client::heapcheck(const CommAddress &addr, String &outfile) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_HEAPCHECK);
  Request::Parameters::Heapcheck params(outfile);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, m_default_timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer heapcheck() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}

void Lib::RangeServer::Client::replay_fragments(const CommAddress &addr, int64_t op_id,
    const String &recover_location, int plan_generation, int32_t type,
    const vector<int32_t> &fragments, const Lib::RangeServerRecovery::ReceiverPlan &plan,
                              int32_t replay_timeout) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_REPLAY_FRAGMENTS);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::ReplayFragments params(op_id, recover_location,
                                              plan_generation, type, fragments,
                                              plan, replay_timeout);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, replay_timeout);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer replay_fragments() failure : ")
             + Hypertable::Protocol::string_format_message(event));

}

void Lib::RangeServer::Client::phantom_load(const CommAddress &addr, const String &location,
                          int32_t plan_generation,
                          const vector<int32_t> &fragments,
                          const vector<QualifiedRangeSpec> &range_specs,
                          const vector<RangeState> &range_states) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_PHANTOM_LOAD);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::PhantomLoad params(location, plan_generation, fragments,
                                          range_specs, range_states);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, m_default_timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer phantom_load() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}

void Lib::RangeServer::Client::phantom_update(const CommAddress &addr, const String &location,
                            int32_t plan_generation, const QualifiedRangeSpec &range,
                            int32_t fragment, StaticBuffer &buffer,
                            DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_PHANTOM_UPDATE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::PhantomUpdate params(location, plan_generation,
                                            range, fragment);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length(), buffer));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void Lib::RangeServer::Client::phantom_prepare_ranges(const CommAddress &addr, int64_t op_id,
                                    const String &location,
                                    int32_t plan_generation, 
                                    const vector<QualifiedRangeSpec> &ranges,
                                    int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_PHANTOM_PREPARE_RANGES);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::PhantomPrepareRanges params(op_id, location,
                                                   plan_generation, ranges);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer phantom_prepare_ranges() failure : ")
             + Hypertable::Protocol::string_format_message(event));

}

void Lib::RangeServer::Client::phantom_commit_ranges(const CommAddress &addr, int64_t op_id,
                                   const String &location, int32_t plan_generation, 
                                   const vector<QualifiedRangeSpec> &ranges,
                                   int32_t timeout_ms) {
  DispatchHandlerSynchronizer sync_handler;
  CommHeader header(Protocol::COMMAND_PHANTOM_COMMIT_RANGES);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::PhantomCommitRanges params(op_id, location,
                                                  plan_generation, ranges);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, &sync_handler, timeout_ms);

  EventPtr event;
  if (!sync_handler.wait_for_reply(event))
    HT_THROW(Hypertable::Protocol::response_code(event),
             String("RangeServer phantom_commit_ranges() failure : ")
             + Hypertable::Protocol::string_format_message(event));
}

void Lib::RangeServer::Client::set_state(const CommAddress &addr,
                       vector<SystemVariable::Spec> &specs, int64_t generation,
                       DispatchHandler *handler, Timer &timer) {
  CommHeader header(Protocol::COMMAND_SET_STATE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::SetState params(specs, generation);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, timer.remaining());
}


void Lib::RangeServer::Client::table_maintenance_enable(const CommAddress &addr,
                                      const TableIdentifier &table,
                                      DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_TABLE_MAINTENANCE_ENABLE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::TableMaintenanceEnable params(table);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}

void Lib::RangeServer::Client::table_maintenance_disable(const CommAddress &addr,
                                       const TableIdentifier &table,
                                       DispatchHandler *handler) {
  CommHeader header(Protocol::COMMAND_TABLE_MAINTENANCE_DISABLE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  Request::Parameters::TableMaintenanceDisable params(table);
  CommBufPtr cbuf(new CommBuf(header, params.encoded_length()));
  params.encode(cbuf->get_data_ptr_address());
  send_message(addr, cbuf, handler, m_default_timeout_ms);
}


void Lib::RangeServer::Client::send_message(const CommAddress &addr, CommBufPtr &cbuf,
                          DispatchHandler *handler, int32_t timeout_ms) {
  int error;

  if ((error = m_comm->send_request(addr, timeout_ms, cbuf, handler))
      != Error::OK) {
    HT_WARNF("Comm::send_request to %s failed - %s",
             addr.to_str().c_str(), Error::get_text(error));
    // COMM_BROKEN_CONNECTION implies handler will get a callback
    if (error != Error::COMM_BROKEN_CONNECTION)
      HT_THROWF(error, "Comm::send_request to %s failed",
                addr.to_str().c_str());
  }
}
