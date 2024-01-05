#include "dbi/FrontierDB.h"

namespace dbi {

    std::mutex FrontierDB::m_conn_mutex;
    std::mutex FrontierDB::m_query_mutex;

}
