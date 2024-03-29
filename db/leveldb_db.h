//
// Created by wujy on 18-1-21.
//

#ifndef YCSB_C_LEVELDB_DB_H
#define YCSB_C_LEVELDB_DB_H

#include "leveldb/db.h"
#include "core/db.h"
#include <string>

using std::string;

namespace ycsbc {
    class LevelDB : public DB{
    public:
        LevelDB(const char *dbfilename);
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        ~LevelDB();

    private:
        leveldb::DB *db_;
        leveldb::ReadOptions readOpt;
        unsigned noResult;
    };
}

#endif //YCSB_C_LEVELDB_DB_H
