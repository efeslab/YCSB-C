//
// Created by wujy on 18-1-21.
//

#include "leveldb_db.h"
#include <iostream>
#include "leveldb/options.h"

using namespace std;

namespace ycsbc {
    LevelDB::LevelDB(const char *dbfilename) :noResult(0){
        leveldb::Options options;
        options.create_if_missing = true;
        options.compression = leveldb::kNoCompression;
        options.write_buffer_size = 128*1024*1024; // 128MB
        options.max_file_size = 128*1024*1024; // 128MB

        leveldb::Status s = leveldb::DB::Open(options,dbfilename,&db_);
        readOpt.verify_checksums = false;
        readOpt.fill_cache = false;
        if(!s.ok()){
            cerr<<"Can't open leveldb: "<<dbfilename<<"Error: "<<s.ToString()<<endl;
            exit(0);
        }
    }

    int LevelDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        for (const std::string &f: *fields) {
            string keyfield(key+f);
            string value;
            //cerr << "Reading keyfield " << keyfield << endl;
            leveldb::Status s = db_->Get(readOpt,keyfield,&value);
            assert(s.ok());
            result.push_back(std::make_pair(f, value));
        }
        return DB::kOK;
    }

    int LevelDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        auto it=db_->NewIterator(leveldb::ReadOptions());
        result.reserve(len);
        assert(!fields->empty());
        it->Seek(key+fields->front());
        unsigned int total_scan = fields->size() * len;
        unsigned int fields_cnt = 0;
        std::vector<std::vector<KVPair>>::iterator result_it = result.begin();
        while (total_scan > 0) {
            assert(it->Valid() && "Invalid iterator before scan finishes");
            result_it->push_back(std::make_pair(it->key().ToString(), it->value().ToString()));
            ++fields_cnt;
            if (fields_cnt == fields->size()) {
                fields_cnt = 0;
                ++result_it;
            }
            it->Next();
        }
        return DB::kOK;
    }

    int LevelDB::Insert(const std::string &table, const std::string &key,
               std::vector<KVPair> &values){
        leveldb::Status s;
        for(KVPair p:values){
            s = db_->Put(leveldb::WriteOptions(),key+p.first,p.second);
            //cerr << "Inserting " << key+p.first << endl;
            if(!s.ok()){
                cerr<<"insert error: " << s.ToString() << endl;
                exit(0);
            }
        }
        return DB::kOK;
    }

    int LevelDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int LevelDB::Delete(const std::string &table, const std::string &key) {
        assert(0 && "Delete with fields not supported");
        leveldb::Status s = db_->Delete(leveldb::WriteOptions(),key);
        if(!s.ok()){
            cerr<<"delete error: "<<s.ToString()<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    LevelDB::~LevelDB() {
        cerr << "Closing leveldb" << endl;
        db_->CompactRange(nullptr, nullptr);
        delete db_;
    }
}
