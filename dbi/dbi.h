#ifndef dbi_dbi_h
#define dbi_dbi_h

/*
 * Description: 
 *    dbi is a lightweight interface to access DB
 *    with different backend. 
 *
 *    The internal design:
 *
 *      +-------+            +------------+
 *      | DBAPI | <>-------- | IDBBackend |
 *      +-------+            +------------+
 *                        _________^_________
 *                       |         |         |
 *                   Frontier    MySQL    SQLite
 *             
 *    Usage:
 *      DummyDB db{}; // this will be copied, so make sure only store simple types 
 *      DBAPI dbapi{db};
 *
 *      The DummyDB is a simple class without any derivation, but need following interfaces:
 *      - doConnect
 *      - doQuery -> fill the results into the ResultSet.m_internals
 *     
 * Author:
 *     Tao Lin <lintao AT ihep.ac.cn>
 */

#include <vector>
#include <tuple>
#include <sstream>
#include <functional>
#include <dbi/util.h>

namespace dbi {

    struct ResultSet {

        // user retrieve the results
        template<typename... Args>
        bool to(std::tuple<Args...>& result) const {
            // convert string to the correct type
            typedef typename std::tuple<Args...> TupleT;

            if (m_internals.size() != std::tuple_size_v<TupleT>) {
                return false;
            }

            tuple_element_helper<std::tuple_size_v<TupleT>, 0>(m_internals, result);
            return true;
        };

        // the backend should fill the results
        std::vector<std::string> m_internals;
    
    };

    // helper class to create record object
    // user need to create their own specialized version
    template<typename T>
    struct dbi_record_object {
        template<typename... Args> static T create(Args... args);

        typedef std::function<decltype(create)> F;
    };

    struct DBAPI {
        // internal interface

        struct IDBBackend {
            virtual bool connect() = 0;
            virtual std::vector<ResultSet> query(const std::string& str) = 0; // do the query
        };

        // internel impl
        // so the different backend don't need to derive from the same interface
        template<typename DB>
        struct DBBackendImpl: IDBBackend {
            DBBackendImpl(DB db): m_db(db) {}
            bool connect() { return m_db.doConnect(); }
            std::vector<ResultSet> query(const std::string& str) { return m_db.doQuery(str); }

            DB m_db;
        };


        template<typename DB>
        DBAPI(DB backend) : m_db_backend(new DBBackendImpl<DB>(backend)) {
        }

        bool connect() {
            return m_db_backend->connect();
        }

        // pass a tuple to the template
        template<typename TupleT>
        auto query_as_tuple(const std::string& str) {

            std::vector<TupleT> results;

            auto records = m_db_backend->query(str);

            for (const auto& record: records) {
                TupleT t;

                record.to(t);

                results.push_back(t);
            }

            return results;
        }

        // pass the arguments to the template
        // these arguments will be packed into a tuple
        template<typename TupleT>
        typename std::enable_if< is_tuple<TupleT>::value != 0, 
        std::vector<TupleT> >::type
        query(const std::string& str) {
            return query_as_tuple<TupleT>(str);
        }


        template<typename Head, typename... Args>
        typename std::enable_if< is_tuple<Head>::value == 0, 
        std::vector<std::tuple<Head, Args...>> >::type
        query(const std::string& str) {
            typedef typename std::tuple<Head, Args...> TupleT;

            return query_as_tuple<TupleT>(str);
        }

        // pass a function to construct struct directly
        // all the types could be deduced
        template<typename T, typename... Args>
        auto query(const std::string& str, 
                   std::function<T(Args...)> f) {
            typedef typename std::tuple<Args...> TupleT;
            std::vector<T> results;

            auto records = m_db_backend->query(str);

            for (const auto& record: records) {
                TupleT t;

                record.to(t);

                results.push_back(std::apply(f, t));
            }

            return results;
        }

        // handle the struct + lambda
        // need to declare type F (std::function<R(Args...)>) in struct
        // the returned type should be specified
        template<typename T, typename Func>
        auto query(const std::string& str, 
                   Func f) {
            return query(str, typename dbi_record_object<T>::F{f});
        }


    private:
        IDBBackend* m_db_backend{nullptr};

    };

}

#endif
