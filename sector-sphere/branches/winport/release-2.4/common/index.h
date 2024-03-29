/*****************************************************************************
Copyright (c) 2005 - 2010, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu, last updated 03/08/2010
*****************************************************************************/


#ifndef __SECTOR_INDEX_H__
#define __SECTOR_INDEX_H__

#include "meta.h"

class COMMON_API Index: public Metadata
{
public:
   Index();
   virtual ~Index();

   virtual void init(const std::string& path) {}
   virtual void clear() {}

public:
   virtual int list(const std::string& path, std::vector<std::string>& filelist);
   virtual int list_r(const std::string& path, std::vector<std::string>& filelist);
   virtual int lookup(const std::string& path, SNode& attr);
   virtual int lookup(const std::string& path, std::set<Address, AddrComp>& addr);

public:
   virtual int create(const std::string& path, bool isdir = false);
   virtual int move(const std::string& oldpath, const std::string& newpath, const std::string& newname = "");
   virtual int remove(const std::string& path, bool recursive = false);
   virtual int update(const std::string& fileinfo, const Address& addr, const int& type);
   virtual int utime(const std::string& path, const int64_t& ts);

public:
   virtual int serialize(const std::string& path, const std::string& dstfile);
   virtual int deserialize(const std::string& path, const std::string& srcfile,  const Address* addr = NULL);
   virtual int scan(const std::string& data_dir, const std::string& meta_dir);

public:
   virtual int merge(const std::string& path, Metadata* branch, const unsigned int& replica);
   virtual int substract(const std::string& path, const Address& addr);
   virtual int64_t getTotalDataSize(const std::string& path);
   virtual int64_t getTotalFileNum(const std::string& path);
   virtual int collectDataInfo(const std::string& path, std::vector<std::string>& result);
   virtual int getUnderReplicated(const std::string& path, std::vector<std::string>& replica, const unsigned int& thresh, const std::map<std::string, int>& special);

private:
   int serialize(std::ofstream& ofs, std::map<std::string, SNode>& currdir, int level);
   int deserialize(std::ifstream& ifs, std::map<std::string, SNode>& currdir, const Address* addr = NULL);
   int scan(const std::string& currdir, std::map<std::string, SNode>& metadata);
   int merge(std::map<std::string, SNode>& currdir, std::map<std::string, SNode>& branch, const unsigned int& replica);
   int substract(std::map<std::string, SNode>& currdir, const Address& addr);
   int64_t getTotalDataSize(std::map<std::string, SNode>& currdir);
   int64_t getTotalFileNum(std::map<std::string, SNode>& currdir);
   int collectDataInfo(const std::string& path, std::map<std::string, SNode>& currdir, std::vector<std::string>& result);
   int getUnderReplicated(const std::string& path, std::map<std::string, SNode>& currdir, std::vector<std::string>& replica, const unsigned int& thresh, const std::map<std::string, int>& special);
   int list_r(std::map<std::string, SNode>& currdir, const std::string& path, std::vector<std::string>& filelist);

private:
   std::map<std::string, SNode> m_mDirectory;
};

#endif
