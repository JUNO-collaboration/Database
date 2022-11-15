#ifndef dbi_MysqlDB_h
#define dbi_MysqlDB_h

#include <dbi/dbi.h>
#include <mysql.h>

#include <memory>

namespace dbi {

    struct MysqlDB {

        MysqlDB(const std::string& _mycnf,
                const std::string& _mycnfgrp)
        : mycnf(_mycnf), mycnfgrp(_mycnfgrp), port(0), client_flag(0) {

        }

        MysqlDB(const std::string& _host, 
                const std::string& _user, 
                const std::string& _pass)
        : host(_host), user(_user), passwd(_pass), port(3306), client_flag(0) {

        }

        bool doConnect() {

            MYSQL* internal_connection{nullptr};
            internal_connection = mysql_init(nullptr);

            if (mycnf.size()) {
                mysql_options(internal_connection, MYSQL_READ_DEFAULT_FILE, (void *)(mycnf.c_str()));
            }
            if (mycnfgrp.size()) {
                mysql_options(internal_connection, MYSQL_READ_DEFAULT_GROUP, (void *)(mycnfgrp.c_str()));
            }

            if (mysql_real_connect(internal_connection, 
                                   host.size() ? host.c_str() : NULL, 
                                   user.size() ? user.c_str() : NULL, 
                                   passwd.size() ? passwd.c_str() : NULL, 
                                   db.size() ? db.c_str() : NULL, 
                                   port,
                                   unix_socket.size() ? unix_socket.c_str() : NULL, 
                                   client_flag) == NULL) {
                std::cerr << "Failed to invoke mysql_real_connect. " << std::endl;
                return false;
            }

            m_connection = std::shared_ptr<MYSQL>{internal_connection};

            return true;
        }

        std::vector<ResultSet> doQuery(const std::string& str) {
            std::vector<ResultSet> results;

            if (mysql_real_query(m_connection.get(), str.c_str(), str.length())) {
                // non-zero: failed
                std::cerr << "Failed to execute mysql_real_query. " << std::endl;
                return results;
            }

            MYSQL_RES* result = mysql_store_result(m_connection.get());
            int num_fields = mysql_num_fields(result);

            MYSQL_ROW row;
            while ((row = mysql_fetch_row(result))) {
                ResultSet rs;

                for (int i = 0; i < num_fields; ++i) {
                    // std::cout << "DEBUG " << row[i] << std::endl;

                    rs.m_internals.push_back(row[i]?row[i]:"");
                }

                results.push_back(rs);
            }

            mysql_free_result(result);

            return results;
        }

    private:
        std::string mycnf;
        std::string mycnfgrp;

        std::string host;
        std::string user;
        std::string passwd;
        std::string db;
        unsigned int port;
        std::string unix_socket;
        unsigned long client_flag;

    private:
        // initialized after doConnect
        std::shared_ptr<MYSQL> m_connection{nullptr};

    };

}

#endif
