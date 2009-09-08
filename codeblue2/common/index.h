/*****************************************************************************
Copyright (c) 2001 - 2009, The Board of Trustees of the University of Illinois.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 01/14/2009
*****************************************************************************/


#ifndef __SECTOR_INDEX_H__
#define __SECTOR_INDEX_H__

#include <string>
#include <vector>
#include <map>
#include <set>
#include <fstream>
#include <topology.h>


class SNode
{
public:
   std::string m_strName;
   bool m_bIsDir;
   std::set<Address, AddrComp> m_sLocation;
   std::map<std::string, SNode> m_mDirectory;
   int64_t m_llTimeStamp;
   int64_t m_llSize;
   std::string m_strChecksum;
   int m_iReadLock;
   int m_iWriteLock;

public:
   int serialize(char* buf);
   int deserialize(const char* buf);
};

class Index
{
friend class Master;
friend class Slave;

public:
   Index();
   ~Index();

public:
   int list(const char* path, std::vector<std::string>& filelist);
   int list_r(const char* path, std::vector<std::string>& filelist);
   int lookup(const char* path, SNode& attr);
   int lookup(const char* path, std::set<Address, AddrComp>& addr);

public:
   int create(const char* path, bool isdir = false);
   int move(const char* oldpath, const char* newpath, const char* newname = NULL);
   int remove(const char* path, bool recursive = false);

      // Functionality:
      //    update the information of a file. e.g., new size, time, or replica.
      // Parameters:
      //    [1] fileinfo: serialized file info
      //    [2] addr: location of the replica to be updated
      //    [3] type: update type, size/time update or new replica
      // Returned value:
      //    number of replicas of the file, or -1 on error.

   int update(const char* fileinfo, const Address& addr, const int& type);
   int utime(const char* path, const int64_t& ts);

public:
   int lock(const char* path, int mode);
   int unlock(const char* path, int mode);

public:
   static int serialize(std::ofstream& ofs, std::map<std::string, SNode>& currdir, int level);
   static int deserialize(std::ifstream& ifs, std::map<std::string, SNode>& currdir, const Address* addr = NULL);
   static int scan(const std::string& currdir, std::map<std::string, SNode>& metadata);

public:
   // NOTE: This set of function requires external mutex protection

      // Functionality:
      //    merge a slave's index with the system file index.
      // Parameters:
      //    1) [in, out] currdir: system directory
      //    2) [in] branch: slave index
      //    3) [in] path: current path (this is a recursive function)
      //    4) [out] left: all the conflict files in the branch
      //    5) [in] replica: number of replicas
      // Returned value:
      //    1 on success, or -1 on error.

   static int merge(std::map<std::string, SNode>& currdir, std::map<std::string, SNode>& branch, std::string path, std::ofstream& left, const unsigned int& replica);
   static int substract(std::map<std::string, SNode>& currdir, const Address& addr);

   static int64_t getTotalDataSize(std::map<std::string, SNode>& currdir);
   static int64_t getTotalFileNum(std::map<std::string, SNode>& currdir);

   int collectDataInfo(const char* file, std::vector<std::string>& result);

public:
   static int parsePath(const char* path, std::vector<std::string>& result);

private:
   static int list_r(std::map<std::string, SNode>& currdir, const std::string& path, std::vector<std::string>& filelist);

private:
   std::map<std::string, SNode> m_mDirectory;
   pthread_mutex_t m_MetaLock;
};

#endif
