#ifndef dbi_DummyDB_h
#define dbi_DummyDB_h

#include <dbi/dbi.h>

namespace dbi {

    struct DummyDB {

        // constructor
        DummyDB() {

        }

        // do connect
        bool doConnect() {

            return true;
        }

        // do query
        std::vector<ResultSet> doQuery(const std::string& str) {
            std::vector<ResultSet> results;

            ResultSet rs;
            rs.m_internals.push_back("1");

            results.push_back(rs);

            return results;
        }


    };

}

#endif
