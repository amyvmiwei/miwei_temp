# 2 RangeServers, 1 crashes and the other one receives all recovered ranges
add_test(RangeServer-failover-basic-1 env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run1.sh)

# 3 RangeServers, 1 crashes and the other 2 receive all recovered ranges
add_test(RangeServer-failover-basic-2 env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run2.sh)

# 5 RangeServers, 2 crash and the other 3 receive all recovered ranges
add_test(RangeServer-failover-basic-3 env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run3.sh)

# 2 RangeServers, 1 crashes and the other one receives all recovered ranges.
# Trying to restart the crashed RangeServer (rs1) will result in a failure
# because it is blocked in Hyperspace
add_test(RangeServer-failover-basic-4 env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run5.sh)

# 6 RangeServers, 4 crash. Recovery is blocked till a quorum of 40% is 
# reached. After 10 seconds, 1 new RangeServer comes up and recovery continues
add_test(RangeServer-failover-quorum env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run4.sh)

# 2 RangeServers and 2 Masters. One Master and one RangeServer crashes. The
# standby master takes over and recovers all ranges to the other RangeServer
add_test(RangeServer-failover-standby-master env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run6.sh)

# 3 RangeServers and 1 Master. Another RangeServer crashes while 
# Recovery for the previously crashed RangeServer is waiting for
# the grace period to pass. The grace period will restart.
add_test(RangeServer-failover-graceperiod env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run7.sh)

# 2 RangeServers, 1 crashes with SIGSTOP and the other one receives 
# all recovered ranges
add_test(RangeServer-failover-sigstop env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run8.sh)

# 2 RangeServers, 1 crashes and is recovered. The failure inducer then
# causes failures when reading log file fragments and the notification hook
# is called
add_test(RangeServer-failover-bad-fragments env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run9.sh)

# 2 RangeServers, cluster is filled and then shutdown. Cluster is
# restarted, but one RangeServer is missing.
add_test(RangeServer-failover-restart env INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run10.sh)

# Fail during load of initial range during create table
add_test(RangeServer-failover-create-table env TEST=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run-create-table-test.sh)

# Fail during table dump, inside RangeServer::create_scanner
add_test(RangeServer-failover-scan-1 env TEST=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run-scan-failover.sh)

# Fail during table dump, inside RangeServer::fetch_scanblock
add_test(RangeServer-failover-scan-2 env TEST=2 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run-scan-failover.sh)
