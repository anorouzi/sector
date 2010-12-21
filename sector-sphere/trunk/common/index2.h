/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/


#ifndef __SECTOR_INDEX2_H__
#define __SECTOR_INDEX2_H__

#include <meta.h>

class Index2: public Metadata
{
public:
   Index2();
   virtual ~Index2();

   virtual void init(const std::string& path);
   virtual void clear();

public:
   virtual int list(const std::string& path, std::vector<std::string>& filelist);
   virtual int list_r(const std::string& path, std::vector<std::string>& filelist);
   virtual int lookup(const std::string& path, SNode& attr);
   virtual int lookup(const std::string& path, std::set<Address, AddrComp>& addr);

public:
   virtual int create(const SNode& node);
   virtual int move(const std::string& oldpath, const std::string& newpath, const std::string& newname = "");
   virtual int remove(const std::string& path, bool recursive = false);
   virtual int addReplica(const std::string& path, const int64_t& ts, const int64_t& size, const Address& addr);
   virtual int removeReplica(const std::string& path, const Address& addr);
   virtual int update(const std::string& path, const int64_t& ts, const int64_t& size = -1);

public:
   virtual int serialize(const std::string& path, const std::string& dstfile);
   virtual int deserialize(const std::string& path, const std::string& srcfile,  const Address* addr = NULL);
   virtual int scan(const std::string& data, const std::string& meta);

public:
   virtual int merge(const std::string& path, Metadata* meta, const unsigned int& replica);
   virtual int substract(const std::string& path, const Address& addr);
   virtual int64_t getTotalDataSize(const std::string& path);
   virtual int64_t getTotalFileNum(const std::string& path);
   virtual int collectDataInfo(const std::string& path, std::vector<std::string>& result);
   virtual int checkReplica(const std::string& path, std::vector<std::string>& under, std::vector<std::string>& over);
   virtual int getSlaveMeta(Metadata* /*branch*/, const Address& /*addr*/) {return 0;}

public:
   virtual void refreshRepSetting(const std::string& /*path*/, int /*default_num*/, int /*default_dist*/, std::map<std::string, int>& /*rep_num*/, std::map<std::string, int>& /*rep_dist*/, std::map<std::string, std::vector<int> >& /*restrict_loc*/) {}

private:
   int serialize(std::ofstream& ofs, const std::string& currdir, int level);
   int merge(const std::string& prefix, const std::string& path, const unsigned int& replica);

private:
   std::string m_strMetaPath;
};

#endif
