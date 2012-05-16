# offloads all ranges from one server to two others (of which one reports
# full disks)
add_test(Defects-issue783-offload1 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run1.sh)

# offloads all ranges from one server to two others (of which both report
# full disks)
add_test(Defects-issue783-offload2 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run2.sh)

# distributes table ranges from one server to two others (of which one reports
# full disks)
add_test(Defects-issue783-ranges1 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run3.sh)

# distributes table ranges from one server to two others (of which both report
# full disks)
add_test(Defects-issue783-ranges2 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run4.sh)

# distributes ranges by load from one server to two others (of which one reports
# full disks)
add_test(Defects-issue783-load1 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run5.sh)

# distributes ranges by load from one server to two others (of which both report
# full disks)
add_test(Defects-issue783-load2 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run6.sh)
