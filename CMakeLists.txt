PKG(dbi
    DEPENDS
        ${MYSQL_CONNECTOR_CPP_LIBRARIES}
        ${MYSQL_CONNECTOR_C_LIBRARIES}
        frontier_client
)
