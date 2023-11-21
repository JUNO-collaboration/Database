#include "dbi/MysqlDB.h"

#ifndef BUILD_ONLINE

namespace dbi {
    std::mutex MysqlDB::m_connect_mutex;
    std::mutex MysqlDB::m_query_mutex;
}

#endif
