//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <cstdio>
#include <vector>
#include <future>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"
using namespace std;
using namespace ycsbc;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
struct DelegateClientResult {
    DelegateClientResult(Client *_client, int _num_ops): client(_client), num_ops(_num_ops) {}
    ~DelegateClientResult() {
        delete client;
    }
    Client *client;
    int num_ops;
};
DelegateClientResult *DelegateClient(DB *db, CoreWorkload *wl, const int num_ops,
    bool is_loading) {
  db->Init();
  Client *client = new Client(*db, *wl);
  int oks = 0;
  for (int i = 0; i < num_ops; ++i) {
    if (is_loading) {
      oks += client->DoInsert();
    } else {
      oks += client->DoTransaction();
    }
  }
  db->Close();
  DelegateClientResult *res = new DelegateClientResult(client, oks);
  return res;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);

  DB *db = DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  CoreWorkload wl;
  wl.Init(props);

  const unsigned int num_threads = stoi(props.GetProperty("threadcount", "1"));

  // Loads data
  vector<future<DelegateClientResult*>> actual_ops;
  int total_ops = stoi(props[CoreWorkload::RECORD_COUNT_PROPERTY]);
  for (unsigned int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, total_ops / num_threads, true));
  }
  assert(actual_ops.size() == num_threads);

  int num_ops_sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    DelegateClientResult *res = n.get();
    num_ops_sum += res->num_ops;
    delete res;
  }
  cout << "# Loading records:\t" << num_ops_sum << endl;

  // Peforms transactions
  actual_ops.clear();
  total_ops = stoi(props[CoreWorkload::OPERATION_COUNT_PROPERTY]);
  utils::Timer<double> timer;
  timer.Start();
  for (unsigned int i = 0; i < num_threads; ++i) {
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, &wl, total_ops / num_threads, false));
  }
  assert(actual_ops.size() == num_threads);

  num_ops_sum = 0;
  uint32_t op_cnt[NUM_OPERATIONS] = {0};
  uint64_t op_timer[NUM_OPERATIONS] = {0};
  uint32_t op_cnt_sum = 0;
  uint64_t op_timer_sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    DelegateClientResult *res = n.get();
    num_ops_sum += res->num_ops;
    for (unsigned int i=0; i < NUM_OPERATIONS; ++i) {
        op_cnt[i] += res->client->op_cnt[i];
        op_cnt_sum += op_cnt[i];
        op_timer[i] += res->client->op_timer[i];
        op_timer_sum += op_timer[i];
    }
  }
  double duration = timer.End();
  cout << "# Transaction throughput (KTPS)" << endl;
  cout << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\t';
  cout << total_ops / duration / 1000 << endl;
  for (unsigned int i=0; i < NUM_OPERATIONS; ++i) {
      fprintf(stderr, "%s, cnt: %u (%.2f%%), timer %lu (%.2f%%), %.2f c/op\n", OperationSTRING[i],
              /*cnt*/ op_cnt[i], (double)(op_cnt[i])/op_cnt_sum,
            /*timer*/ op_timer[i], (double)(op_timer[i])/op_timer_sum,
           /* c/op */ (double(op_timer[i])/op_cnt[i]));
  }
  fprintf(stderr, "SUM, cnt: %u, timer: %lu, %.2f c/op\n", op_cnt_sum, op_timer_sum,
      (double)(op_timer_sum)/op_cnt_sum);
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-dbfilename")==0){
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbfilename", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

