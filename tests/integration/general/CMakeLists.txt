add_test(HQL-set env INSTALL_DIR=${INSTALL_DIR}
                 ${CMAKE_CURRENT_SOURCE_DIR}/hql-set-test.sh)

add_test(HQL-counter env INSTALL_DIR=${INSTALL_DIR}
                     ${CMAKE_CURRENT_SOURCE_DIR}/hql-counter-test.sh)

add_test(HQL-incompatible-column-options
        env INSTALL_DIR=${INSTALL_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/hql-incompatible-column-options.sh)

add_test(HQL-delete
        env INSTALL_DIR=${INSTALL_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/hql-delete-test.sh)
