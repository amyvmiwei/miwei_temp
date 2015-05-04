#
# Copyright (C) 2007-2015 Hypertable, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

set(Hypertable_SRCS
AccessGroupSpec.cc
ApacheLogParser.cc
BalancePlan.cc
BlockCompressionCodec.cc
BlockCompressionCodecBmz.cc
BlockCompressionCodecLzo.cc
BlockCompressionCodecNone.cc
BlockCompressionCodecQuicklz.cc
BlockCompressionCodecSnappy.cc
BlockCompressionCodecZlib.cc
BlockHeader.cc
BlockHeaderCellStore.cc
BlockHeaderCommitLog.cc
Canonicalize.cc
Cell.cc
CellInterval.cc
Client.cc
ClusterId.cc
ColumnFamilySpec.cc
ColumnPredicate.cc
CommitLog.cc
CommitLogBlockStream.cc
CommitLogReader.cc
CompressorFactory.cc
Config.cc
DataGenerator.cc
FixedRandomStringGenerator.cc
Future.cc
HqlCommandInterpreter.cc
HqlHelpText.cc
HqlInterpreter.cc
IndexTables.cc
IntervalScannerAsync.cc
Key.cc
KeySpec.cc
LegacyDecoder.cc
LoadDataEscape.cc
LoadDataSource.cc
LoadDataSourceFactory.cc
LoadDataSourceFileDfs.cc
LoadDataSourceFileLocal.cc
LoadDataSourceStdin.cc
LocationCache.cc
Master/Client.cc
Master/EventHandlerMasterChange.cc
Master/HyperspaceCallback.cc
Master/NamespaceFlag.cc
Master/Request/Parameters/AlterTable.cc
Master/Request/Parameters/Balance.cc
Master/Request/Parameters/Compact.cc
Master/Request/Parameters/CreateNamespace.cc
Master/Request/Parameters/CreateTable.cc
Master/Request/Parameters/DropNamespace.cc
Master/Request/Parameters/DropTable.cc
Master/Request/Parameters/FetchResult.cc
Master/Request/Parameters/MoveRange.cc
Master/Request/Parameters/PhantomCommitComplete.cc
Master/Request/Parameters/PhantomPrepareComplete.cc
Master/Request/Parameters/RecreateIndexTables.cc
Master/Request/Parameters/RegisterServer.cc
Master/Request/Parameters/RelinquishAcknowledge.cc
Master/Request/Parameters/RenameTable.cc
Master/Request/Parameters/ReplayComplete.cc
Master/Request/Parameters/ReplayStatus.cc
Master/Request/Parameters/SetState.cc
Master/Request/Parameters/Stop.cc
Master/Response/Parameters/RegisterServer.cc
Master/Response/Parameters/Status.cc
Master/Response/Parameters/SystemStatus.cc
MetaLog.cc
MetaLogEntity.cc
MetaLogEntityHeader.cc
MetaLogReader.cc
MetaLogWriter.cc
NameIdMapper.cc
Namespace.cc
NamespaceCache.cc
ProfileDataScanner.cc
PseudoTables.cc
QualifiedRangeSpec.cc
RS_METRICS/RangeMetrics.cc
RS_METRICS/ReaderTable.cc
RS_METRICS/ServerMetrics.cc
RangeLocator.cc
RangeMoveSpec.cc
RangeServer/Client.cc
RangeServer/Protocol.cc
RangeServer/Request/Parameters/AcknowledgeLoad.cc
RangeServer/Request/Parameters/CommitLogSync.cc
RangeServer/Request/Parameters/Compact.cc
RangeServer/Request/Parameters/CreateScanner.cc
RangeServer/Request/Parameters/DestroyScanner.cc
RangeServer/Request/Parameters/DropRange.cc
RangeServer/Request/Parameters/DropTable.cc
RangeServer/Request/Parameters/Dump.cc
RangeServer/Request/Parameters/DumpPseudoTable.cc
RangeServer/Request/Parameters/FetchScanblock.cc
RangeServer/Request/Parameters/GetStatistics.cc
RangeServer/Request/Parameters/Heapcheck.cc
RangeServer/Request/Parameters/LoadRange.cc
RangeServer/Request/Parameters/PhantomCommitRanges.cc
RangeServer/Request/Parameters/PhantomLoad.cc
RangeServer/Request/Parameters/PhantomPrepareRanges.cc
RangeServer/Request/Parameters/PhantomUpdate.cc
RangeServer/Request/Parameters/RelinquishRange.cc
RangeServer/Request/Parameters/ReplayFragments.cc
RangeServer/Request/Parameters/SetState.cc
RangeServer/Request/Parameters/TableMaintenanceDisable.cc
RangeServer/Request/Parameters/TableMaintenanceEnable.cc
RangeServer/Request/Parameters/Update.cc
RangeServer/Request/Parameters/UpdateSchema.cc
RangeServer/Response/Parameters/AcknowledgeLoad.cc
RangeServer/Response/Parameters/CreateScanner.cc
RangeServer/Response/Parameters/GetStatistics.cc
RangeServer/Response/Parameters/Status.cc
RangeServerRecovery/FragmentReplayPlan.cc
RangeServerRecovery/Plan.cc
RangeServerRecovery/ReceiverPlan.cc
RangeServerRecovery/ReplayPlan.cc
RangeServerRecovery/ServerReceiverPlan.cc
RangeSpec.cc
RangeState.cc
Result.cc
RootFileHandler.cc
RowInterval.cc
ScanBlock.cc
ScanCells.cc
ScanSpec.cc
Schema.cc
StatsRangeServer.cc
StatsTable.cc
SystemVariable.cc
Table.cc
TableCache.cc
TableCallback.cc
TableDumper.cc
TableIdentifier.cc
TableMutator.cc
TableMutatorAsync.cc
TableMutatorAsyncDispatchHandler.cc
TableMutatorAsyncHandler.cc
TableMutatorAsyncScatterBuffer.cc
TableMutatorFlushHandler.cc
TableMutatorIntervalHandler.cc
TableMutatorShared.cc
TableMutatorSyncDispatchHandler.cc
TableParts.cc
TableScanner.cc
TableScannerAsync.cc
TableScannerDispatchHandler.cc
TableScannerHandler.cc
TableSplit.cc
TestSource.cc
bmz/bmz.c
)

add_library(Hypertable ${Hypertable_SRCS})
add_dependencies(Hypertable HyperComm Hyperspace HyperCommon)
target_link_libraries(Hypertable ${EXPAT_LIBRARIES} Hyperspace HyperFsBroker ${MALLOC_LIBRARY} HyperThirdParty m)

# ColumnFamilySpec_test
add_executable(ColumnFamilySpec_test tests/ColumnFamilySpec_test.cc)
target_link_libraries(ColumnFamilySpec_test Hypertable)

# AccessGroupSpec_test
add_executable(AccessGroupSpec_test tests/AccessGroupSpec_test.cc)
target_link_libraries(AccessGroupSpec_test Hypertable)

# Schema_test
add_executable(Schema_test tests/Schema_test.cc)
target_link_libraries(Schema_test Hypertable)

# locationCacheTest
add_executable(locationCacheTest tests/locationCacheTest.cc)
target_link_libraries(locationCacheTest Hypertable)

# loadDataSourceTest
add_executable(loadDataSourceTest tests/loadDataSourceTest.cc)
target_link_libraries(loadDataSourceTest Hypertable)

# compressor_test
add_executable(compressor_test tests/compressor_test.cc)
target_link_libraries(compressor_test Hypertable)

# bmz binaries
add_executable(bmz-test bmz/bmz-test.c)
if (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
   target_link_libraries(bmz-test Hypertable m)
else (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
   target_link_libraries(bmz-test Hypertable)
endif (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")

add_executable(bmzip bmz/bmzip.c)
if (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
   target_link_libraries(bmzip Hypertable m)
else (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
   target_link_libraries(bmzip Hypertable m)
endif (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")

# block_header_test
add_executable(block_header_test tests/block_header_test.cc)
target_link_libraries(block_header_test Hypertable)

# commit_log_test
add_executable(commit_log_test tests/commit_log_test.cc)
target_link_libraries(commit_log_test HyperFsBroker Hypertable)

# escape_test
add_executable(escape_test tests/escape_test.cc)
target_link_libraries(escape_test Hypertable)

# large_insert_test
add_executable(large_insert_test tests/large_insert_test.cc)
target_link_libraries(large_insert_test Hypertable)

# async_api_test 
add_executable(async_api_test tests/async_api_test.cc)
target_link_libraries(async_api_test Hypertable)

# scanner_abrupt_end_test
add_executable(scanner_abrupt_end_test tests/scanner_abrupt_end_test.cc)
target_link_libraries(scanner_abrupt_end_test Hypertable)

# future_abrupt_end_test
add_executable(future_abrupt_end_test tests/future_abrupt_end_test.cc)
target_link_libraries(future_abrupt_end_test Hypertable)

# future_mutator_cancel_test
add_executable(future_mutator_cancel_test tests/future_mutator_cancel_test.cc)
target_link_libraries(future_mutator_cancel_test Hypertable)

# future_test 
add_executable(future_test tests/future_test.cc)
target_link_libraries(future_test Hypertable)

# key_spec_test 
add_executable(key_spec_test tests/key_spec_test.cc)
target_link_libraries(key_spec_test Hypertable)

# scan_spec_test
add_executable(scan_spec_test tests/scan_spec_test.cc)
target_link_libraries(scan_spec_test Hypertable)

# indices_test
add_executable(indices_test tests/indices_test.cc)
target_link_libraries(indices_test Hypertable)

# row_delete_test
add_executable(row_delete_test tests/row_delete_test.cc)
target_link_libraries(row_delete_test Hypertable)

# MutatorNoLogSyncTest
add_executable(MutatorNoLogSyncTest tests/MutatorNoLogSyncTest.cc)
target_link_libraries(MutatorNoLogSyncTest Hypertable)

# periodic_flush_test
add_executable(periodic_flush_test tests/periodic_flush_test.cc)
target_link_libraries(periodic_flush_test Hypertable)

# name_id_mapper_test 
add_executable(name_id_mapper_test tests/name_id_mapper_test.cc)
target_link_libraries(name_id_mapper_test Hypertable Hyperspace)

# rangeserver_serialize_test 
add_executable(rangeserver_serialize_test tests/rangeserver_serialize_test.cc)
target_link_libraries(rangeserver_serialize_test Hypertable Hyperspace)

# MetaLog test
add_executable(metalog_test tests/metalog_test.cc)
target_link_libraries(metalog_test HyperFsBroker Hypertable)


#
# Copy test files
#
set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hypertable/Lib/tests")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable/Lib")
configure_file(${SRC_DIR}/test-schemas.xml ${DST_DIR}/test-schemas.xml COPYONLY)

configure_file(${HYPERTABLE_SOURCE_DIR}/tests/data/random.dat
               ${DST_DIR}/random.dat COPYONLY)
configure_file(${SRC_DIR}/ColumnFamilySpec_test.golden ${DST_DIR}/ColumnFamilySpec_test.golden)
configure_file(${SRC_DIR}/AccessGroupSpec_test.golden ${DST_DIR}/AccessGroupSpec_test.golden)
configure_file(${SRC_DIR}/Schema_test.golden ${DST_DIR}/Schema_test.golden)
configure_file(${SRC_DIR}/indices_test.golden ${DST_DIR}/indices_test.golden)
configure_file(${SRC_DIR}/locationCacheTest.golden
               ${DST_DIR}/locationCacheTest.golden)
configure_file(${SRC_DIR}/asyncApiTest.golden
               ${DST_DIR}/asyncApiTest.golden)
configure_file(${SRC_DIR}/loadDataSourceTest.golden
               ${DST_DIR}/loadDataSourceTest.golden)
configure_file(${SRC_DIR}/loadDataSourceTest.dat
               ${DST_DIR}/loadDataSourceTest.dat)
configure_file(${SRC_DIR}/loadDataSourceTest-header.golden
               ${DST_DIR}/loadDataSourceTest-header.golden)
configure_file(${SRC_DIR}/loadDataSourceTest-header.dat
               ${DST_DIR}/loadDataSourceTest-header.dat)
configure_file(${SRC_DIR}/loadDataSourceTest-qualified-header.golden
               ${DST_DIR}/loadDataSourceTest-qualified-header.golden)
configure_file(${SRC_DIR}/loadDataSourceTest-qualified-header.dat
               ${DST_DIR}/loadDataSourceTest-qualified-header.dat)
configure_file(${HYPERTABLE_SOURCE_DIR}/conf/hypertable.cfg
               ${DST_DIR}/hypertable.cfg)
configure_file(${SRC_DIR}/future_test.cfg ${DST_DIR}/future_test.cfg)
configure_file (${SRC_DIR}/MutatorNoLogSyncTest.cfg ${DST_DIR}/MutatorNoLogSyncTest.cfg)
configure_file(${SRC_DIR}/name_id_mapper_test.cfg ${DST_DIR}/name_id_mapper_test.cfg)
configure_file(${SRC_DIR}/metalog_test.golden ${DST_DIR}/metalog_test.golden)
configure_file(${SRC_DIR}/metalog_test2.golden ${DST_DIR}/metalog_test2.golden)
configure_file(${SRC_DIR}/metalog_test3.golden ${DST_DIR}/metalog_test3.golden)

add_test(Hypertable-Lib-TestSetup env INSTALL_DIR=${INSTALL_DIR}
         ${SRC_DIR}/test_setup.sh)
add_test(ColumnFamilySpec ColumnFamilySpec_test)
add_test(AccessGroupSpec AccessGroupSpec_test)
add_test(Schema Schema_test)
add_test(LocationCache locationCacheTest)
add_test(LoadDataSource loadDataSourceTest)
add_test(LoadDataEscape escape_test)
add_test(BlockCompressor-BMZ compressor_test bmz)
add_test(BlockCompressor-LZO compressor_test lzo)
add_test(BlockCompressor-NONE compressor_test none)
add_test(BlockCompressor-QUICKLZ compressor_test quicklz)
add_test(BlockCompressor-ZLIB compressor_test zlib)
add_test(BlockCompressor-SNAPPY compressor_test snappy)
add_test(BlockHeader block_header_test)
add_test(CommitLog commit_log_test)
add_test(MetaLog metalog_test)
add_test(Client-large-block large_insert_test)
add_test(Client-async-api async_api_test)
add_test(Client-future future_test)
add_test(Client-row-delete row_delete_test)
add_test(Client-periodic-flush periodic_flush_test)
add_test(Keyspec env INSTALL_DIR=${INSTALL_DIR} ${CMAKE_CURRENT_BINARY_DIR}/key_spec_test)
add_test(NameIdMapper name_id_mapper_test --config=${DST_DIR}/name_id_mapper_test.cfg)
add_test(StatsRangeServer-serialize rangeserver_serialize_test)
add_test(ScanSpec-basic-tests scan_spec_test)
add_test(Secondary-Indices-tests indices_test)

if (NOT HT_COMPONENT_INSTALL)
  file(GLOB HEADERS *.h)
  install(FILES ${HEADERS}
        DESTINATION include/Hypertable/Lib)
  file(GLOB HEADERS RangeServerRecovery/*.h)
  install(FILES ${HEADERS}
          DESTINATION include/Hypertable/Lib/RangeServerRecovery)
  file(GLOB HEADERS RangeServer/*.h)
  install(FILES ${HEADERS}
          DESTINATION include/Hypertable/Lib/RangeServer)
  file(GLOB HEADERS RangeServer/Request/Parameters/*.h)
  install(FILES ${HEADERS}
          DESTINATION include/Hypertable/Lib/RangeServer/Request/Parameters)
  file(GLOB HEADERS RangeServer/Response/Parameters/*.h)
  install(FILES ${HEADERS}
          DESTINATION include/Hypertable/Lib/RangeServer/Response/Parameters)

  file(GLOB HEADERS Master/*.h)
  install(FILES ${HEADERS}
          DESTINATION include/Hypertable/Lib/Master)
  file(GLOB HEADERS Master/Request/Parameters/*.h)
  install(FILES ${HEADERS}
          DESTINATION include/Hypertable/Lib/Master/Request/Parameters)
  file(GLOB HEADERS Master/Response/Parameters/*.h)
  install(FILES ${HEADERS}
          DESTINATION include/Hypertable/Lib/Master/Response/Parameters)
  install(TARGETS Hypertable
      LIBRARY DESTINATION lib
      ARCHIVE DESTINATION lib)
endif ()
