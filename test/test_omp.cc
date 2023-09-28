// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <omp.h>

#include <atomic>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>

// ./test_omp --gtest_list_tests
// user@dingo6 bin [main] $ ./test_omp --gtest_list_tests
// Running main() from /home/user/work/dingo-store/contrib/gtest/googletest/src/gtest_main.cc
// OmpTest.
//   Test1Default
//   Test2Limit10
//   Test3Limit50
//   Test4ThreadUse
//   Test5UseEnv
//   Test6Display
//   Test7ThreadLimit7
//   Test8ThreadStorm
//   Test9SetDynamic
//   Test10MultiTimes
//   Test11MultiThread

// ./test_omp --gtest_filter=OmpTest.Test1Default
// ./test_omp --gtest_filter=OmpTest.Test2Limit10
// ./test_omp --gtest_filter=OmpTest.Test3Limit50
// ./test_omp --gtest_filter=OmpTest.Test4ThreadUse

// export OMP_NUM_THREADS=2
// ./test_omp --gtest_filter=OmpTest.Test5UseEnv

// export OMP_DISPLAY_ENV=TRUE
// ./test_omp --gtest_filter=OmpTest.Test6Display

// export OMP_THREAD_LIMIT=3
// ./test_omp --gtest_filter=OmpTest.Test7ThreadLimit7

// ./test_omp --gtest_filter=OmpTest.Test8ThreadStorm
// ./test_omp --gtest_filter=OmpTest.Test9SetDynamic
// ./test_omp --gtest_filter=OmpTest.Test10MultiTimes
// ./test_omp --gtest_filter=OmpTest.Test11MultiThread

std::mutex t;
std::atomic_uint32_t cnt{0};
std::map<pthread_t, int> mp_thread;

class OmpTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

void PrintOmp() {
  std::lock_guard<std::mutex> lg(t);
  std::cout << "omp_get_num_threads : " << omp_get_num_threads() << std::endl;
  std::cout << "omp_get_max_threads : " << omp_get_max_threads() << std::endl;
  std::cout << "omp_get_thread_num : " << omp_get_thread_num() << std::endl;
  std::cout << "omp_get_num_procs : " << omp_get_num_procs() << std::endl;
  std::cout << "No : " << cnt << " thread : " << pthread_self() << std::endl;
  std::cout << std::endl;
  cnt++;

  auto iter = mp_thread.find(pthread_self());
  if (iter != mp_thread.end()) {
    iter->second++;
  } else {
    mp_thread[pthread_self()] = 1;
  }
}

void PrintMp() {
  int i = 0;
  for (const auto& elem : mp_thread) {
    std::cout << "[" << i++ << "]"
              << "tid:" << elem.first << " count:" << elem.second << std::endl;
  }
}

void ClearMp() { mp_thread.clear(); }

void ClearCnt() { cnt = 0; }

// default
// Use the default parameters to start 48 threads by default
TEST(OmpTest, Test1Default) {
  ClearCnt();
  ClearMp();
  std::cout << "test1_default" << std::endl;
  PrintOmp();
  ClearCnt();
  ClearMp();
  std::cout << "........................................................." << std::endl;
#pragma omp parallel
  { PrintOmp(); }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// limit10
// Set 10 threads The default concurrency is 10 threads
TEST(OmpTest, Test2Limit10) {
  ClearCnt();
  ClearMp();
  std::cout << "test2_limit10" << std::endl;
  omp_set_num_threads(10);
  PrintOmp();
  ClearCnt();
  ClearMp();
  std::cout << "........................................................." << std::endl;
#pragma omp parallel
  { PrintOmp(); }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// limit50
// Set 50 threads The default concurrency is 50 threads
TEST(OmpTest, Test3Limit50) {
  ClearCnt();
  ClearMp();
  std::cout << "test3_limit50" << std::endl;
  omp_set_num_threads(50);
  PrintOmp();
  ClearCnt();
  ClearMp();
  std::cout << "........................................................." << std::endl;
#pragma omp parallel
  { PrintOmp(); }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// Set 50 threads for num_threads(52) to 52
// num_threads as many as you want This parameter plays a key role
TEST(OmpTest, Test4ThreadUse) {
  ClearCnt();
  ClearMp();
  std::cout << "test4_thread_use" << std::endl;
  omp_set_num_threads(50);
  PrintOmp();
  ClearCnt();
  ClearMp();
  std::cout << "........................................................." << std::endl;
#pragma omp parallel num_threads(52)
  { PrintOmp(); }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// Use the ENV parameter to test the priority
// omp_set_num_threads > OMP_NUM_THREADS
// export OMP_NUM_THREADS=2
// This cannot be verified.
TEST(OmpTest, Test5UseEnv) {
  ClearCnt();
  ClearMp();
  // setenv("OMP_NUM_THREADS", "2", 1);
  // auto* value = getenv("OMP_NUM_THREADS");
  // std::cout << "OMP_NUM_THREADS : " << value << std::endl;
  std::cout << "test5_use_env" << std::endl;
  omp_set_num_threads(5);
  PrintOmp();
  ClearCnt();
  ClearMp();
  std::cout << "........................................................." << std::endl;
#pragma omp parallel
  { PrintOmp(); }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// First print default parameter set
// export OMP_DISPLAY_ENV=TRUE
// OPENMP DISPLAY ENVIRONMENT BEGIN
//   _OPENMP = '201511'
//   OMP_DYNAMIC = 'FALSE'
//   OMP_NESTED = 'FALSE'
//   OMP_NUM_THREADS = '48'
//   OMP_SCHEDULE = 'DYNAMIC'
//   OMP_PROC_BIND = 'FALSE'
//   OMP_PLACES = ''
//   OMP_STACKSIZE = '0'
//   OMP_WAIT_POLICY = 'PASSIVE'
//   OMP_THREAD_LIMIT = '4294967295'
//   OMP_MAX_ACTIVE_LEVELS = '2147483647'
//   OMP_CANCELLATION = 'FALSE'
//   OMP_DEFAULT_DEVICE = '0'
//   OMP_MAX_TASK_PRIORITY = '0'
//   OMP_DISPLAY_AFFINITY = 'FALSE'
//   OMP_AFFINITY_FORMAT = 'level %L thread %i affinity %A'
// OPENMP DISPLAY ENVIRONMENT END
TEST(OmpTest, Test6Display) {
  ClearCnt();
  ClearMp();
  std::cout << "test6_display" << std::endl;
}

// export OMP_THREAD_LIMIT=3
// This parameter has the highest priority and maximum concurrency of 3
// {
//   user@dingo6 cpp $ export OMP_THREAD_LIMIT=3
// user@dingo6 cpp $ ./11-test-omp
// test7_thread_limit
// omp_get_num_threads : 1
// omp_get_max_threads : 15
// omp_get_thread_num : 0
// omp_get_num_procs : 48
// No : 0 thread : 140698855471040

// .........................................................
// omp_get_num_threads : 3
// omp_get_max_threads : 15
// omp_get_thread_num : 1
// omp_get_num_procs : 48
// No : 0 thread : 140698833045248

// omp_get_num_threads : 3
// omp_get_max_threads : 15
// omp_get_thread_num : 0
// omp_get_num_procs : 48
// No : 1 thread : 140698855471040

// omp_get_num_threads : 3
// omp_get_max_threads : 15
// omp_get_thread_num : 2
// omp_get_num_procs : 48
// No : 2 thread : 140698824652544

// cnt : 3
// [0]tid:140698824652544 count:1
// [1]tid:140698833045248 count:1
// [2]tid:140698855471040 count:1
// }
TEST(OmpTest, Test7ThreadLimit7) {
  ClearCnt();
  ClearMp();
  std::cout << "test7_thread_limit" << std::endl;
  omp_set_num_threads(15);
  PrintOmp();
  ClearCnt();
  ClearMp();
  std::cout << "........................................................." << std::endl;
#pragma omp parallel num_threads(40)
  { PrintOmp(); }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// Did not occur actually 5 threads were used
void FunB() {
#pragma omp parallel for
  for (int i = 0; i < 5; i++) {
    PrintOmp();
  }
}
TEST(OmpTest, Test8ThreadStorm) {
  ClearCnt();
  ClearMp();
#pragma omp parallel for
  for (int i = 0; i < 5; i++) {
    FunB();
  }
  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// Did not actually use 5 threads // Set dynamic threads to ensure that the number of threads does not exceed the CPU
// core count.
// omp_set_dynamic(1); activate.
// omp_set_dynamic(0); Off.
// Recommended to enable.
// {
//   test4_thread_use
// omp_get_num_threads : 1
// omp_get_max_threads : 5
// omp_get_thread_num : 0
// omp_get_num_procs : 48
// No : 0 thread : 140382394665984

// .........................................................
// omp_get_num_threads : 5
// omp_get_max_threads : 5
// omp_get_thread_num : 1
// omp_get_num_procs : 48
// No : 0 thread : 140382372235008

// omp_get_num_threads : 5
// omp_get_max_threads : 5
// omp_get_thread_num : 2
// omp_get_num_procs : 48
// No : 1 thread : 140382363842304

// omp_get_num_threads : 5
// omp_get_max_threads : 5
// omp_get_thread_num : 3
// omp_get_num_procs : 48
// No : 2 thread : 140382355449600

// omp_get_num_threads : 5
// omp_get_max_threads : 5
// omp_get_thread_num : 0
// omp_get_num_procs : 48
// No : 3 thread : 140382394665984

// omp_get_num_threads : 5
// omp_get_max_threads : 5
// omp_get_thread_num : 4
// omp_get_num_procs : 48
// No : 4 thread : 140382347056896

// cnt : 5
// [0]tid:140382347056896 count:1
// [1]tid:140382355449600 count:1
// [2]tid:140382363842304 count:1
// [3]tid:140382372235008 count:1
// [4]tid:140382394665984 count:1
// }
TEST(OmpTest, Test9SetDynamic) {
  ClearCnt();
  ClearMp();
  std::cout << "test9_thread_use" << std::endl;
  omp_set_dynamic(1);
  omp_set_num_threads(5);
  PrintOmp();
  ClearCnt();
  ClearMp();
  std::cout << "........................................................." << std::endl;
#pragma omp parallel num_threads(52)
  { PrintOmp(); }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// Multiple calls parallel
// The previous thread appears to have an internal thread pool
TEST(OmpTest, Test10MultiTimes) {
  ClearCnt();
  ClearMp();
  std::cout << "test10_multi_times" << std::endl;

  std::cout << "........................................................." << std::endl;
#pragma omp parallel
  { PrintOmp(); }

#pragma omp parallel
  { PrintOmp(); }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

// Multiple calls parallel
// Start thread storm Thread pools are not used
TEST(OmpTest, Test11MultiThread) {
  ClearCnt();
  ClearMp();
  std::cout << "test11_multi_thread" << std::endl;

  std::cout << "........................................................." << std::endl;
  std::vector<std::thread> vt_thread;

  vt_thread.reserve(2);
  for (int i = 0; i < 2; i++) {
    vt_thread.push_back(std::thread([]() {
#pragma omp parallel
      { PrintOmp(); }
#pragma omp parallel
      { PrintOmp(); }
    }));
  }

  for (auto& t : vt_thread) {
    t.join();
  }

  std::cout << "cnt : " << cnt << std::endl;
  PrintMp();
}

TEST(OmpTest, DISABLED_Test12Dummy) { std::cout << __FUNCTION__ << std::endl; }