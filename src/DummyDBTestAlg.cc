#include <SniperKernel/AlgBase.h>
#include <SniperKernel/AlgFactory.h>

#include <dbi/dbi.h>
#include <dbi/DummyDB.h>
#include <dbi/FrontierDB.h>
#include <dbi/MysqlDB.h>
#include <dbi/SqliteDB.h>

struct GlobalTag {
    std::string SftVer;
    std::string CondGTag;
    std::string ParaGTag;
    std::string Creator;
    std::string CreateTime;

    // typedef std::function<GlobalTag(std::string, std::string, std::string, std::string, std::string)> F;

};

namespace dbi {
    template<>
    struct dbi_record_object<GlobalTag> {
        static GlobalTag create(std::string sftver, 
                                std::string condgtag, 
                                std::string paragtag, 
                                std::string creator, 
                                std::string createtime) {
            return GlobalTag{sftver, condgtag, paragtag, creator, createtime};
        }

        typedef std::function<decltype(create)> F; // necessry
    };

}

struct DummyDBTestConfig {
    std::string frontier_url = "http://junodb1.ihep.ac.cn:8000/Frontier";
    std::string proxy_url = "http://junosquid01.ihep.ac.cn:3128";

    std::string frontier_legacy_url = "http://junodb1.ihep.ac.cn:8080/Frontier";

    std::string mysql_server = "junodb1.ihep.ac.cn";

    std::string stmt_select_one = "select 1 from dual";
    std::string stmt_select_one_sqlite = "select 1";
    std::string stmt_select_time = "select current_timestamp()";
    std::string stmt_select_date = "select DATE_FORMAT(now(),'%Y-%m-%d_%H:%i')";

    std::string stmt_select_id_spms = "select id from spms.global_tag";

    std::string stmt_select_sftver = "select sftver from OfflineDB.GlobalTag";
    std::string stmt_select_serno = "select serno from OfflineDB.GlobalTag";

    std::string stmt_select_globaltag = "select sftver,cond_gtag,para_gtag,creator,create_time from OfflineDB.GlobalTag";
};

struct DummyDBTestAlg: public AlgBase {

    DummyDBTestAlg(const std::string& name)
        : AlgBase(name) {
        declProp("User", m_user);
        declProp("Pass", m_pass);
        declProp("mycnf", m_mycnf);
        declProp("mycnfgrp", m_mycnfgrp);
        declProp("sqlitefn", m_sqlitefn);
    }

    ~DummyDBTestAlg() {

    }

    bool initialize() {

        dbi::DummyDB dummydb{};

        m_dbapi = new dbi::DBAPI(dummydb);

        dbi::FrontierDB frontierdb{m_config.frontier_legacy_url};

        m_frontierapi = new dbi::DBAPI(frontierdb);
        m_frontierapi->connect();


        dbi::FrontierDB frontierproxydb{m_config.frontier_url, 
                                        m_config.proxy_url};

        m_frontierproxyapi = new dbi::DBAPI(frontierproxydb);
        m_frontierproxyapi->connect();


        if (m_mycnf.size()) { // use my.cnf
            dbi::MysqlDB mysqldb{m_mycnf, m_mycnfgrp};
            m_mysqlapi = new dbi::DBAPI(mysqldb);
        } else if (m_user.size() and m_pass.size()) { // use 
            dbi::MysqlDB mysqldb{m_config.mysql_server, m_user, m_pass};
            m_mysqlapi = new dbi::DBAPI(mysqldb);
        } else {
            LogError << "Neither my.cnf file nor user/pass are specified. "
                     << std::endl;
            return false;
        }

        if (m_mysqlapi->connect()) {
            LogInfo << "MySQL DB Connected. " << std::endl;
        } else {
            LogWarn << "MySQL DB connected failed. " << std::endl;
        }

        if (m_sqlitefn.size()) {
            dbi::SqliteDB sqlitedb{m_sqlitefn};
            m_sqliteapi = new dbi::DBAPI(sqlitedb);
            m_sqliteapi->connect();
        }
        
        return true;
    }

    bool execute() {

        std::cout << "DummyDB: " << std::endl;
        auto results = m_dbapi->query<int>(m_config.stmt_select_one);

        std::cout << "size: " << results.size() << std::endl;

        for (auto result: results) {
            auto [one] = result;
            std::cout << "one: " << one << std::endl;
        }

        std::cout << "FrontierDB: " << std::endl;
        results = m_frontierapi->query<int>(m_config.stmt_select_one);
        for (auto result: results) {
            auto [one] = result;
            std::cout << "one: " << one << std::endl;
        }


        std::cout << "FrontierDB global tag: " << std::endl;
        auto results_gt = m_frontierapi->query<std::string>(m_config.stmt_select_id_spms);
        for (auto result: results_gt) {
            auto [one] = result;
            std::cout << "one: " << one << std::endl;
        }

        std::cout << "FrontierDB global tag (OfflineDB): " << std::endl;
        auto results_gt_offlinedb = m_frontierapi->query<std::string>(m_config.stmt_select_sftver);
        for (auto result: results_gt_offlinedb) {
            auto [one] = result;
            std::cout << "one: " << one << std::endl;
        }


        auto results_globaltag_offlinedb = 
        m_frontierapi->query<std::string, // SftVer
                             std::string, // CondGTag
                             std::string, // ParaGTag
                             std::string, // Creator
                             std::string  // CreateTime
                             >(m_config.stmt_select_globaltag);

        for (auto result: results_globaltag_offlinedb) {
            auto [sftver, condgtag, paragtag, creator, createtime] = result;
            std::cout << "sftver: " << sftver
                      << " condgtag: " << condgtag
                      << " paragtag: " << paragtag
                      << " creator: " << creator
                      << " createtime: " << createtime
                      << std::endl;
        }


        
        dbi::dbi_record_object<GlobalTag>::F convert_to_gt = 
        [](std::string sftver, 
           std::string condgtag, 
           std::string paragtag, 
           std::string creator, 
           std::string createtime) -> GlobalTag{
            return GlobalTag{sftver, condgtag, paragtag, creator, createtime};
        };

        std::string stmt_globaltag_offlinedb = m_config.stmt_select_globaltag;
        for (auto result: m_frontierapi->query(stmt_globaltag_offlinedb, convert_to_gt)) {
            std::cout << "GlobalTag: "
                      << " SftVer: " << result.SftVer
                      << " CondGTag: " << result.CondGTag
                      << " ParaGTag: " << result.ParaGTag
                      << " Creator: " << result.CreateTime
                      << std::endl;
        }

        for (auto result: m_frontierapi->query(stmt_globaltag_offlinedb, 
                                               dbi::dbi_record_object<GlobalTag>::F{
                                                   dbi::dbi_record_object<GlobalTag>::create})) {
            std::cout << "GlobalTag: "
                      << " SftVer: " << result.SftVer
                      << " CondGTag: " << result.CondGTag
                      << " ParaGTag: " << result.ParaGTag
                      << " Creator: " << result.CreateTime
                      << std::endl;
        }

        for (auto result: m_frontierapi->query<GlobalTag>(stmt_globaltag_offlinedb, 
                                               [](std::string sftver, 
                                                  std::string condgtag, 
                                                  std::string paragtag, 
                                                  std::string creator, 
                                                  std::string createtime) -> GlobalTag{
                                                   return GlobalTag{sftver, condgtag, paragtag, creator, createtime};
                                                  }
                                               )) {
            std::cout << "GlobalTag: "
                      << " SftVer: " << result.SftVer
                      << " CondGTag: " << result.CondGTag
                      << " ParaGTag: " << result.ParaGTag
                      << " Creator: " << result.CreateTime
                      << std::endl;
        }

        using GlobalTag_t = std::tuple<std::string, 
                                       std::string, 
                                       std::string, 
                                       std::string, 
                                       std::string>;

        for (auto result: m_frontierapi->query<GlobalTag_t>(stmt_globaltag_offlinedb)) {
            auto [sftver, condgtag, paragtag, creator, createtime] = result;
            std::cout << "sftver: " << sftver
                      << " condgtag: " << condgtag
                      << " paragtag: " << paragtag
                      << " creator: " << creator
                      << " createtime: " << createtime
                      << std::endl;

        }



        std::cout << "MysqlDB global tag (OfflineDB): " << std::endl;
        for (auto result: m_mysqlapi->query<std::string>(m_config.stmt_select_sftver)) {
            auto [one] = result;
            std::cout << "one: " << one << std::endl;
        }

        for (auto result: 
                 m_mysqlapi->query<
                 GlobalTag
                 >(stmt_globaltag_offlinedb, 
                   [](std::string sftver, 
                      std::string condgtag, 
                      std::string paragtag, 
                      std::string creator, 
                      std::string createtime) -> GlobalTag{
                       return GlobalTag{sftver, condgtag, paragtag, creator, createtime};
                   }
                   )) {
            std::cout << "GlobalTag: "
                      << " SftVer: " << result.SftVer
                      << " CondGTag: " << result.CondGTag
                      << " ParaGTag: " << result.ParaGTag
                      << " Creator: " << result.CreateTime
                      << std::endl;
        }



        for (auto result: 
                 m_mysqlapi->query<
                 int
                 >(m_config.stmt_select_serno)) {
            auto [serno] = result;
            std::cout << "GlobalTag: "
                      << " SerNO: " << serno
                      << std::endl;
        }

        for (auto result: 
                 m_mysqlapi->query<
                 std::string
                 >(m_config.stmt_select_time)) {
            auto [current_timestamp] = result;
            std::cout << "Current Timestamp: "
                      << " current_timestamp(): " << current_timestamp
                      << std::endl;
        }


        std::cout << "Testing the current DBAPIs: " << std::endl;
        std::vector<dbi::DBAPI*> vec_dbapi{m_dbapi, m_frontierapi, m_frontierproxyapi, m_mysqlapi};

        for (auto dbapi: vec_dbapi) {
            for (auto result: 
                     dbapi->query<
                     std::string
                     >(m_config.stmt_select_date)) {
                auto [current_timestamp] = result;
                std::cout << "Current Timestamp: "
                          << " current_timestamp(): " << current_timestamp
                          << std::endl;
            }
        }

        if (m_sqliteapi) {
            std::cout << "sqlite: " << std::endl;
            for (auto result: m_sqliteapi->query<int>(m_config.stmt_select_one_sqlite)) {
                auto [one] = result;
                std::cout << "one: " << one << std::endl;
            }
        }

        return true;
    }

    bool finalize() {
        return true;
    }

    dbi::DBAPI* m_dbapi{nullptr};
    dbi::DBAPI* m_frontierapi{nullptr};
    dbi::DBAPI* m_frontierproxyapi{nullptr};
    dbi::DBAPI* m_mysqlapi{nullptr};
    dbi::DBAPI* m_sqliteapi{nullptr};


    std::string m_host;
    std::string m_user;
    std::string m_pass;

    std::string m_mycnf;
    std::string m_mycnfgrp;

    std::string m_sqlitefn;

    DummyDBTestConfig m_config;
};

DECLARE_ALGORITHM(DummyDBTestAlg);

