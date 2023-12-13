#ifndef dbi_FrontierDB_h
#define dbi_FrontierDB_h

#include <dbi/dbi.h>
#include <frontier_client/frontier-cpp.h>

#include <memory>
#include <mutex>
#include <iostream>

namespace dbi {

    struct FrontierDB {

        // constructor
        FrontierDB(const std::string& url) 
            : m_server_urls({url}) {

        }

        FrontierDB(const std::string& url, const std::string& proxy_url) 
            : m_server_urls({url}), m_proxy_urls({proxy_url}) {

        }

        FrontierDB(const std::list<std::string>& urls, const std::list<std::string>& proxy_urls) 
            : m_server_urls(urls), m_proxy_urls(proxy_urls) {

        }

        // doConnect
        bool doConnect() {
            std::cout << "Frontier Connections: " << std::endl;
            std::cout << "  -- Server: " << std::endl;
            for (auto& url: m_server_urls) {
                std::cout << "    -- " << url << std::endl;
            }
            std::cout << "  -- Proxy: " << std::endl;
            for (auto& url: m_proxy_urls) {
                std::cout << "    -- " << url << std::endl;
            }
            m_connection = std::make_shared<frontier::Connection>(m_server_urls, m_proxy_urls);

            return true;
        }

        // doQuery
        std::vector<ResultSet> doQuery(const std::string& str) {
            std::scoped_lock<std::mutex> lock(m_query_mutex);

            frontier::Session sess(m_connection.get());

            const std::string param=frontier::Request::encodeParam(str);
            const std::string req_data="frontier_request:1:DEFAULT";

            frontier::Request req(req_data,frontier::BLOB);
            req.addKey("p1",param);

            std::vector<const frontier::Request*> vrq;
            vrq.push_back(&req);

            sess.getData(vrq);
            sess.setCurrentLoad(1);

            sess.next();
            // MetaData consists of one record with field names.
            // Let's go over all fields:
            std::string name,type;
      
            int field_num = 0;

            while(!sess.isEOR()) {
                sess.assignString(&name);
                sess.assignString(&type);
                ++field_num;
                std::cout<<field_num<<" "<<(name)<<" "<<(type)<<std::endl;
            }

            std::vector<ResultSet> results;

            int nrec=sess.getNumberOfRecords();
            std::cout<<"\nResult contains "<< nrec<<" objects.\n";
            
            while(sess.next()) {
                ResultSet rs;
                for(int k=0;k<field_num;k++) {
                    frontier::AnyData ad;
                    sess.getAnyData(&ad);

                    std::string val;
                    if (!ad.getRawStrP()) {
                        // NULL
                        val = "";
                    } else if (ad.getRawStrS() == 0) {
                        val = "";
                    } else {
                        std::string* vs = ad.getString();
                        val = *vs;
                    }

                    rs.m_internals.push_back(val);
                }
                results.push_back(rs);
            }


            return results;
        }

    private:
        // configurations
        std::list<std::string> m_server_urls;
        std::list<std::string> m_proxy_urls;

    private:
        // initialized after doConnect
        // std::unique_ptr<frontier::Connection> m_connection{nullptr};
        std::shared_ptr<frontier::Connection> m_connection{nullptr};

    private:
        static std::mutex m_query_mutex;

    };

}

#endif
