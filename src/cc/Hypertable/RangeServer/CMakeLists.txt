#
# Copyright (C) 2007-2013 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

add_subdirectory(tests)

set(RangeServer_SRCS
AccessGroup.cc
AccessGroupGarbageTracker.cc
AccessGroupHintsFile.cc
CellCache.cc
CellCacheAllocator.cc
CellCacheManager.cc
CellCacheScanner.cc
CellListScannerBuffer.cc
CellStoreReleaseCallback.cc
CellStoreFactory.cc
CellStoreScanner.cc
CellStoreScannerIntervalBlockIndex.cc
CellStoreScannerIntervalReadahead.cc
CellStoreTrailerV0.cc
CellStoreTrailerV1.cc
CellStoreTrailerV2.cc
CellStoreTrailerV3.cc
CellStoreTrailerV4.cc
CellStoreTrailerV5.cc
CellStoreTrailerV6.cc
CellStoreTrailerV7.cc
CellStore.cc
CellStoreV0.cc
CellStoreV1.cc
CellStoreV2.cc
CellStoreV3.cc
CellStoreV4.cc
CellStoreV5.cc
CellStoreV6.cc
CellStoreV7.cc
Config.cc
ConnectionHandler.cc
FileBlockCache.cc
FillScanBlock.cc
FragmentData.cc
Global.cc
GroupCommit.cc
GroupCommitTimerHandler.cc
HyperspaceSessionHandler.cc
IndexUpdater.cc
KeyCompressorNone.cc
KeyCompressorPrefix.cc
KeyDecompressorNone.cc
KeyDecompressorPrefix.cc
LiveFileTracker.cc
LoadMetricsRange.cc
LocationInitializer.cc
MaintenancePrioritizer.cc
MaintenancePrioritizerLogCleanup.cc
MaintenancePrioritizerLowMemory.cc
MaintenanceQueue.cc
MaintenanceScheduler.cc
MaintenanceTaskCompaction.cc
MaintenanceTaskDeferredInitialization.cc
MaintenanceTaskMemoryPurge.cc
MaintenanceTaskRelinquish.cc
MaintenanceTaskSplit.cc
MaintenanceTaskWorkQueue.cc
MergeScanner.cc
MergeScannerRange.cc
MergeScannerAccessGroup.cc
MetaLogEntityRange.cc
MetaLogEntityRemoveOkLogs.cc
MetaLogEntityTaskAcknowledgeRelinquish.cc
MetaLogDefinitionRangeServer.cc
MetadataNormal.cc
MetadataRoot.cc
PhantomRange.cc
PhantomRangeMap.cc
QueryCache.cc
Range.cc
RangeServer.cc
RequestHandlerAcknowledgeLoad.cc
RequestHandlerCompact.cc
RequestHandlerCreateScanner.cc
RequestHandlerDestroyScanner.cc
RequestHandlerDoMaintenance.cc
RequestHandlerDropRange.cc
RequestHandlerDump.cc
RequestHandlerDumpPseudoTable.cc
RequestHandlerGetStatistics.cc
RequestHandlerGroupCommit.cc
RequestHandlerFetchScanblock.cc
RequestHandlerHeapcheck.cc
RequestHandlerDropTable.cc
RequestHandlerLoadRange.cc
RequestHandlerMetadataSync.cc
RequestHandlerUpdateSchema.cc
RequestHandlerRelinquishRange.cc
RequestHandlerStatus.cc
RequestHandlerUpdate.cc
RequestHandlerWaitForMaintenance.cc
RequestHandlerShutdown.cc
RequestHandlerCommitLogSync.cc
RequestHandlerReplayFragments.cc
RequestHandlerPhantomLoad.cc
RequestHandlerPhantomUpdate.cc
RequestHandlerPhantomCommitRanges.cc
RequestHandlerPhantomPrepareRanges.cc
RequestHandlerSetState.cc
ResponseCallbackCreateScanner.cc
ResponseCallbackFetchScanblock.cc
ResponseCallbackGetStatistics.cc
ResponseCallbackUpdate.cc
ResponseCallbackPhantomUpdate.cc
ResponseCallbackAcknowledgeLoad.cc
RangeReplayBuffer.cc
ReplayBuffer.cc
ReplayDispatchHandler.cc
ScanContext.cc
ScannerMap.cc
ServerState.cc
TableInfo.cc
TableInfoMap.cc
TableSchemaCache.cc
TimerHandler.cc
UpdateThread.cc
)

if (USE_TCMALLOC)
  add_definitions(-DCLEAN_SHUTDOWN)
endif ()

# RangeServer Lib
add_library(HyperRanger ${RangeServer_SRCS})
target_link_libraries(HyperRanger m HyperDfsBroker Hypertable ${RE2_LIBRARIES})

# RangeServer
add_executable(Hypertable.RangeServer main.cc)
target_link_libraries(Hypertable.RangeServer HyperRanger ${MALLOC_LIBRARY})

# csdump
add_executable(csdump csdump.cc)
target_link_libraries(csdump HyperRanger)

# csvalidate
add_executable(csvalidate csvalidate.cc)
target_link_libraries(csvalidate HyperRanger)

# count_stored - program to diff two sorted files
add_executable(count_stored count_stored.cc)
target_link_libraries(count_stored HyperRanger)

if (NOT HT_COMPONENT_INSTALL)
  file(GLOB HEADERS *.h)
  install(FILES ${HEADERS}
      DESTINATION include/Hypertable/RangeServer)
  install(TARGETS HyperRanger Hypertable.RangeServer csdump csvalidate count_stored
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
endif ()

