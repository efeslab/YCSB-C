//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <string>
#include <cstring>
#include <x86intrin.h>
#include "db.h"
#include "core_workload.h"
#include "utils.h"
#define TIMER(t, b) {uint64_t tmp=__rdtsc(); b t+=__rdtsc() - tmp;}
namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {
      std::memset(op_timer, 0, sizeof(op_timer));
      std::memset(op_cnt, 0, sizeof(op_cnt));
  }
  
  virtual bool DoInsert();
  virtual bool DoTransaction();
  
  virtual ~Client() { }
  uint32_t op_cnt[NUM_OPERATIONS];
  uint64_t op_timer[NUM_OPERATIONS];
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();
  
  DB &db_;
  CoreWorkload &workload_;
};

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  //for (DB::KVPair &p: pairs) {
  //    std::cerr << "key: " << key << " field: " << p.first << " value: " << p.second << std::endl;
  //}
  return (db_.Insert(workload_.NextTable(), key, pairs) == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      break;
    case UPDATE:
      status = TransactionUpdate();
      break;
    case INSERT:
      status = TransactionInsert();
      break;
    case SCAN:
      status = TransactionScan();
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  int ret;
  ++op_cnt[READ];
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(workload_.NextFieldName());
    TIMER(op_timer[READ], ret = db_.Read(table, key, &fields, result););
    //for (DB::KVPair &p: result) {
    //    std::cerr << "Field(" << p.first.size() << "): " << p.first << " Value(" << p.second.size() << "): " << p.second << std::endl;
    //}
  } else {
    TIMER(op_timer[READ], ret = db_.Read(table, key, NULL, result););
  }
  return ret;
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  int ret;

  ++op_cnt[READMODIFYWRITE];
  uint64_t begin_tsc = __rdtsc();
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  ret = db_.Update(table, key, values);
  op_timer[READMODIFYWRITE] += __rdtsc() - begin_tsc;
  return ret;
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  int ret;
  ++op_cnt[SCAN];
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back(workload_.NextFieldName());
    TIMER(op_timer[SCAN], ret = db_.Scan(table, key, len, &fields, result););
  } else {
    TIMER(op_timer[SCAN], ret = db_.Scan(table, key, len, NULL, result););
  }
  return ret;
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  int ret;
  ++op_cnt[UPDATE];
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  TIMER(op_timer[UPDATE], ret = db_.Update(table, key, values););
  return ret;
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  int ret;
  ++op_cnt[INSERT];
  workload_.BuildValues(values);
  TIMER(op_timer[INSERT], ret = db_.Insert(table, key, values););
  return ret;
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
