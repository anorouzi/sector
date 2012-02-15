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


#ifndef __SECTOR_INDEX_H__
#define __SECTOR_INDEX_H__

#include <meta.h>
#include <osportable.h>

namespace sector
{

class Index: public Metadata
{
struct STree;

public:
   Index();
   virtual ~Index();

   virtual void init(const std::string& /*path*/) {}
   virtual void clear() {}

public:
   virtual int list(const std::string& path, std::vector<std::string>& filelist);
   virtual int list_r(const std::string& path, std::vector<std::string>& filelist);
   virtual int lookup(const std::string& path, SNode& attr);
   virtual int lookup(const std::string& path, std::set<Address, AddrComp>& addr);

public:
   virtual int create(const std::string& path, const SNode& node);
   virtual int create(const SNode& node);
   virtual int rename(const std::string& src, const std::string& dst);
   virtual int remove(const std::string& path, bool recursive = false);
   virtual int addReplica(const std::string& path, const int64_t& ts, const int64_t& size, const Address& addr);
   virtual int removeReplica(const std::string& path, const Address& addr);
   virtual int update(const std::string& path, const int64_t& ts, const int64_t& size = -1);

public:
   virtual int serialize(const std::string& path, const std::string& dstfile);
   virtual int deserialize(const std::string& path, const std::string& srcfile,  const Address* addr = NULL);
   virtual int scan(const std::string& data_dir, const std::string& meta_dir);

public:
   virtual int merge(Metadata* branch, const unsigned int& replica);
   virtual int substract(const std::string& path, const Address& addr);

   virtual int64_t getTotalDataSize(const std::string& path);
   virtual int64_t getTotalFileNum(const std::string& path);
   virtual int collectDataInfo(const std::string& path, std::vector<std::string>& result);
   virtual int checkReplica(const std::string& path, std::vector<std::string>& under, std::vector<std::string>& over);
   virtual int getSlaveMeta(Metadata* branch, const Address& addr);

public:
   virtual void refreshRepSetting(const std::string& path, int default_num, int default_dist,
                                  std::map<std::string, int>& rep_num, std::map<std::string, int>& rep_dist,
                                  std::map<std::string, std::vector<int> >& restrict_loc);

private:
   STree* lookup(const std::vector<std::string>& dir_vec);
   STree* lookup(const std::string& path);
   STree* lookupParent(const std::string& path);
   STree* create(const std::string& path);

   int serialize(std::ofstream& ofs, STree* tree, int level);
   int deserialize(std::ifstream& ifs, STree* tree, const Address* addr = NULL);
   int scan(const std::string& currdir, STree* tree);
   int merge(STree* tree, STree* branch, const unsigned int& replica);
   int substract(STree* parent, STree* tree, const Address& addr);

   int64_t getTotalDataSize(const STree& tree) const;
   int64_t getTotalFileNum(const STree& tree) const;
   int collectDataInfo(const std::string& path, const STree* parent, const STree* tree, std::vector<std::string>& result) const;
   int checkReplica(const std::string& path, const STree& tree, std::vector<std::string>& under, std::vector<std::string>& over) const;
   int list_r(const STree& tree, const std::string& path, std::vector<std::string>& filelist) const;
   int getSlaveMeta(const STree& tree, const std::vector<std::string>& path, STree* target, const Address& addr) const;

   int refreshRepSetting(const std::string& path, STree* tree, int default_num, int default_dist,
                         std::map<std::string, int>& rep_num, std::map<std::string, int>& rep_dist,
                         std::map<std::string, std::vector<int> >& restrict_loc);

private:
   struct STree
   {
      SNode m_Node;
      std::map<std::string, STree> m_mDirectory;
      void clear() {m_mDirectory.clear();}
   } m_MetaTree;

   // TODO: we should local different part of the tree to increase concurrency.
   RWLock m_MetaLock;
};

}  // namespace sector

#endif
