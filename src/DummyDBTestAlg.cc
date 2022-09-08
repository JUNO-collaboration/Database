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

        std::string url = "http://junodb1.ihep.ac.cn:8080/Frontier";
        dbi::FrontierDB frontierdb{url};

        m_frontierapi = new dbi::DBAPI(frontierdb);
        m_frontierapi->connect();


        if (m_mycnf.size()) { // use my.cnf
            dbi::MysqlDB mysqldb{m_mycnf, m_mycnfgrp};
            m_mysqlapi = new dbi::DBAPI(mysqldb);
        } else if (m_user.size() and m_pass.size()) { // use 
            dbi::MysqlDB mysqldb{"junodb1.ihep.ac.cn", m_user, m_pass};
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
        auto results = m_dbapi->query<int>("select 1 from dual");

        std::cout << "size: " << results.size() << std::endl;

        for (auto result: results) {
            auto [one] = result;
            std::cout << "one: " << one << std::endl;
        }

        std::cout << "FrontierDB: " << std::endl;
        results = m_frontierapi->query<int>("select 1 from dual");
        for (auto result: results) {
            auto [one] = result;
            std::cout << "one: " << one << std::endl;
        }


        std::cout << "FrontierDB global tag: " << std::endl;
        auto results_gt = m_frontierapi->query<std::string>("select id from spms.global_tag");
        for (auto result: results_gt) {
            auto [one] = result;
            std::cout << "one: " << one << std::endl;
        }

        std::cout << "FrontierDB global tag (OfflineDB): " << std::endl;
        auto results_gt_offlinedb = m_frontierapi->query<std::string>("select SftVer from OfflineDB.GlobalTag");
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
                             >("select SftVer,CondGTag,ParaGTag,Creator,CreateTime from OfflineDB.GlobalTag");

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

        std::string stmt_globaltag_offlinedb = 
            "select SftVer,CondGTag,ParaGTag,Creator,CreateTime from OfflineDB.GlobalTag";
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
        for (auto result: m_mysqlapi->query<std::string>("select SftVer from OfflineDB.GlobalTag")) {
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
                 >("select SerNO from OfflineDB.GlobalTag")) {
            auto [serno] = result;
            std::cout << "GlobalTag: "
                      << " SftNO: " << serno
                      << std::endl;
        }

        for (auto result: 
                 m_mysqlapi->query<
                 std::string
                 >("select current_timestamp()")) {
            auto [current_timestamp] = result;
            std::cout << "Current Timestamp: "
                      << " current_timestamp(): " << current_timestamp
                      << std::endl;
        }


        std::cout << "Testing the current DBAPIs: " << std::endl;
        std::vector<dbi::DBAPI*> vec_dbapi{m_dbapi, m_frontierapi, m_mysqlapi};

        for (auto dbapi: vec_dbapi) {
            for (auto result: 
                     dbapi->query<
                     std::string
                     >("select DATE_FORMAT(now(),'%Y-%m-%d_%H:%i')")) {
                auto [current_timestamp] = result;
                std::cout << "Current Timestamp: "
                          << " current_timestamp(): " << current_timestamp
                          << std::endl;
            }
        }

        if (m_sqliteapi) {
            std::cout << "sqlite: " << std::endl;
            for (auto result: m_sqliteapi->query<int>("select 1")) {
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
    dbi::DBAPI* m_mysqlapi{nullptr};
    dbi::DBAPI* m_sqliteapi{nullptr};


    std::string m_host;
    std::string m_user;
    std::string m_pass;

    std::string m_mycnf;
    std::string m_mycnfgrp;

    std::string m_sqlitefn;
};

DECLARE_ALGORITHM(DummyDBTestAlg);

