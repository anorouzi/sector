/*****************************************************************************
Copyright 2011 VeryCloud LLC

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   bdl62, last updated 05/21/2010
*****************************************************************************/

#include "assert.h"
#include <iostream>

#include "index.h"

using namespace sector;
using namespace std;

int test1()
{
   Address addr1;
   addr1.m_strIP = "192.168.0.1";
   addr1.m_iPort = 9000;
   Address addr2;
   addr2.m_strIP = "192.168.0.2";
   addr2.m_iPort = 9000;

   SNode s;
   s.m_strName = "test_file";
   s.m_llTimeStamp = 1000000;
   s.m_llSize = 10;
   s.m_sLocation.insert(addr1);

   // Initialize.
   Index index;
   assert(index.getTotalFileNum("/") == 0);
   assert(index.getTotalDataSize("/") == 0);

   // Test "create" and "list".
   index.create("/", s);
   vector<string> filelist;
   index.list("/", filelist);
   assert(filelist.size() == 1);
   index.create("1/2/3/", s);
   index.list("/", filelist);
   assert(filelist.size() == 2);
   index.list("/1/2/3/", filelist);
   assert(filelist.size() == 1);
   index.list_r("/", filelist);
   assert(filelist.size() == 2);

   // Test "lookup".
   SNode r;
   assert(index.lookup("noexist", r) < 0);
   index.lookup("/1/2/3/test_file", r);
   assert(s.m_strName == r.m_strName);
   assert(r.m_sLocation.begin()->m_strIP == "192.168.0.1");

   // Test "rename".
   index.rename("test_file", "test_file_new");
   assert(index.lookup("test_file", r) < 0);
   assert(index.lookup("test_file_new", r) == 0);
   index.rename("/1/2/3/test_file", "/10/20/30/test_file");
   assert(index.lookup("/1/2/3/test_file", r) < 0);
   assert(index.lookup("/1/2/3", r) == 0);
   assert(index.lookup("/10/20/30/test_file", r) == 0);

   // Test "remove".
   index.remove("test_file_new");
   assert(index.lookup("test_file_new", r) < 0);

   // Test "add/remove replica".
   index.addReplica("10/20/30/test_file", 1000000, 10, addr2);
   index.lookup("/10/20/30/test_file", r);
   assert(r.m_sLocation.size() == 2);
   index.removeReplica("10/20/30/test_file", addr2);
   index.lookup("/10/20/30/test_file", r);
   assert(r.m_sLocation.size() == 1);

   // Test "update".
   index.update("/10/20/30/test_file", 2000000, 200);
   index.lookup("/10/20/30/test_file", r);
   assert(r.m_llTimeStamp == 2000000);
   assert(r.m_llSize == 200);

   // Check total size and number of files again.
   assert(index.getTotalFileNum("/") == 1);
   assert(index.getTotalDataSize("/") == 200);   

   return 0;
}

int test2()
{
   // Test "scan".
   LocalFS::rmdir("/tmp/index_unittest/");
   LocalFS::mkdir("/tmp/index_unittest/");
   LocalFS::mkdir("/tmp/index_unittest/10/");
   LocalFS::mkdir("/tmp/index_unittest/10/20");
   LocalFS::mkdir("/tmp/index_unittest/10/20/30");

   SNode s;
   s.m_strName = "test_file";
   s.m_llTimeStamp = 1000000;
   s.m_llSize = 10; 
   LocalFS::create("/tmp/index_unittest", s);
   LocalFS::create("/tmp/index_unittest/10/20/30", s);

   Index index;
   index.scan("/tmp/index_unittest/", "/");
   assert(index.getTotalFileNum("/") == 2);
   assert(index.getTotalDataSize("/") == 20);
   SNode r;
   assert(index.lookup("test_file", r) == 0);
   assert(index.lookup("/10/20/30/test_file", r) == 0);

   // Test "serialize/deserialize".
   index.serialize("/", "/tmp/sector.common.index.test.dat");
   Index index2;
   index2.deserialize("/", "/tmp/sector.common.index.test.dat");
   assert(index2.getTotalFileNum("/") == 2);
   assert(index2.getTotalDataSize("/") == 20);
   assert(index2.lookup("test_file", r) == 0);
   assert(index2.lookup("/10/20/30/test_file", r) == 0);

   return 0;
}

int test3()
{
   Address addr1;
   addr1.m_strIP = "192.168.0.1";
   addr1.m_iPort = 9000;
   Address addr2;
   addr2.m_strIP = "192.168.0.2";
   addr2.m_iPort = 9000;

   SNode s;
   s.m_strName = "test_file";
   s.m_llTimeStamp = 1000000;
   s.m_llSize = 10;
   s.m_sLocation.insert(addr1);

   // Test "merge" and "substract".
   Index index;
   index.create("/", s);
   Index index2;
   s.m_sLocation.erase(addr1);
   s.m_sLocation.insert(addr2);
   index2.create("/", s);
   index2.create("10/20/30", s);
   index.merge(&index2, 2);
   assert(index.getTotalFileNum("/") == 2);
   assert(index.getTotalDataSize("/") == 20);
   SNode r;
   assert(index.lookup("test_file", r) == 0);
   assert(r.m_sLocation.size() == 2);
   assert(index.lookup("/10/20/30/test_file", r) == 0);

   index.substract("/", addr2);
   assert(index.getTotalFileNum("/") == 1);
   assert(index.lookup("test_file", r) == 0);
   assert(r.m_sLocation.size() == 1);
   assert(index.lookup("/10/20/30/test_file", r) < 0);

   return 0;
}

int main()
{
   test1();
   test2();
   test3();
}
