/****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 03/16/2011
*****************************************************************************/

#include <algorithm>
#include <cstring>
#include <iostream>
#include <queue>
#include <set>
#include <sstream>
#include <stack>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include "common.h"
#include "index.h"
#include "sector.h"

using namespace std;
using namespace sector;

Index::Index()
{
   // Initialize root node.
   SNode& s = m_MetaTree.m_Node;
   s.m_strName = "";
   s.m_bIsDir = true;
   s.m_llSize = 0;
   s.m_llTimeStamp = time(NULL);
}

Index::~Index()
{
}

int Index::list(const string& path, vector<string>& filelist)
{
   filelist.clear();

   RWGuard mg(m_MetaLock, RW_READ);

   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;

   // This is a file.
   if (!tree->m_Node.m_bIsDir)
   {
      char* buf = NULL;
      if (tree->m_Node.serialize(buf) >= 0)
         filelist.push_back(buf);
      delete [] buf;
      return filelist.size();
   }

   // This is a directory.
   for (map<string, STree>::const_iterator i = tree->m_mDirectory.begin();
        i != tree->m_mDirectory.end(); ++ i)
   {
      char* buf = NULL;
      if (i->second.m_Node.serialize(buf) >= 0)
         filelist.push_back(buf);
      delete [] buf;
   }

   return filelist.size();
}

int Index::list_r(const string& path, vector<string>& filelist)
{
   filelist.clear();

   RWGuard mg(m_MetaLock, RW_READ);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;

   return list_r(*tree, path, filelist);
}

int Index::lookup(const string& path, SNode& attr)
{
   RWGuard mg(m_MetaLock, RW_READ);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;
   attr = tree->m_Node;
   return tree->m_mDirectory.size();
}

int Index::lookup(const string& path, set<Address, AddrComp>& addr)
{
   RWGuard mg(m_MetaLock, RW_READ);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;

   queue<const STree*> scanmap;
   scanmap.push(tree);
   while (!scanmap.empty())
   {
      const STree* t = scanmap.front();
      scanmap.pop();

      if (t->m_Node.m_bIsDir)
      {
         for (map<string, STree>::const_iterator i = t->m_mDirectory.begin(); i != t->m_mDirectory.end(); ++ i)
            scanmap.push(&i->second);
      }
      else
      {
         for (set<Address, AddrComp>::const_iterator i = t->m_Node.m_sLocation.begin(); i != t->m_Node.m_sLocation.end(); ++ i)
            addr.insert(*i);
      }
   }

   return addr.size();
}

int Index::create(const string& path, const SNode& node)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   STree* tree = create(path);
   if (tree == NULL)
      return -1;

   STree new_tree;
   new_tree.m_Node = node;
   return tree->m_mDirectory.insert(pair<string, STree>(node.m_strName, new_tree)).second;
}

int Index::create(const SNode& node)
{
   string abs_path = node.m_strName;
   SNode real_node = node;
   real_node.m_strName = getNodeName(abs_path);
   return create(getPathName(abs_path), real_node);
}

int Index::rename(const string& src, const string& dst)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   STree* src_parent = lookupParent(src);
   if (src_parent == NULL)
      return -1;
   string name = getNodeName(src);
   map<string, STree>::iterator t = src_parent->m_mDirectory.find(name);
   if (t == src_parent->m_mDirectory.end())
      return -1;

   STree* dst_tree = lookup(dst);

   // POSIT semantic: if source is a directory but the destination
   // is a non-empty directory, return error.
   if (t->second.m_Node.m_bIsDir &&
       (dst_tree != NULL) &&
       dst_tree->m_Node.m_bIsDir &&
       !dst_tree->m_mDirectory.empty())
   {
      return -1;
   }

   bool dst_exist = true;
   if (dst_tree == NULL)
   {
      dst_tree = create(dst);
      dst_exist = false;
   }

   if (!dst_exist || !dst_tree->m_Node.m_bIsDir)
   {
     // If dst does not exist or it is a file, replace dst with src,
     // but keep its name.
     string orig_name = dst_tree->m_Node.m_strName;
     dst_tree->m_Node = t->second.m_Node;
     dst_tree->m_mDirectory = t->second.m_mDirectory;
     dst_tree->m_Node.m_strName = orig_name;
   }
   else
   {
     // Otherwise if dst is a directory,
     // put the moved files under the dst directory.
     dst_tree->m_mDirectory[t->first] = t->second;
   }

   // Remove source.
   src_parent->m_mDirectory.erase(t);

   return 0;
}

int Index::remove(const string& path, bool recursive)
{
   RWGuard mg(m_MetaLock, RW_WRITE);
   STree* tree = lookup(path);
   STree* parent = lookupParent(path);
   if (!tree || !parent)
      return -1;

   if (!tree->m_mDirectory.empty() && !recursive)
      return -1;

   parent->m_mDirectory.erase(getNodeName(path));

   return 0;
}

int Index::addReplica(const string& path, const int64_t& ts, const int64_t& size, const Address& addr)
{
   RWGuard mg(m_MetaLock, RW_WRITE);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;
   if (tree->m_Node.m_bIsDir)
      return -1;
   if ((tree->m_Node.m_llSize != size) || (tree->m_Node.m_llTimeStamp != ts))
      return -1;
   tree->m_Node.m_sLocation.insert(addr);
   return 0;
}

int Index::removeReplica(const string& path, const Address& addr)
{
   RWGuard mg(m_MetaLock, RW_WRITE);

   STree* parent = lookupParent(path);
   if (parent == NULL)
      return -1;
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;

   if (tree->m_Node.m_bIsDir)
      return -1;
   tree->m_Node.m_sLocation.erase(addr);
   if (tree->m_Node.m_sLocation.empty())
      parent->m_mDirectory.erase(tree->m_Node.m_strName);

   return 0;
}

int Index::update(const string& path, const int64_t& ts, const int64_t& size)
{
   RWGuard mg(m_MetaLock, RW_WRITE);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;

   // sometime it may be necessary to update timestamp only. In this case size should be set to <0.
   if (size >= 0)
      tree->m_Node.m_llSize = size;
   tree->m_Node.m_llTimeStamp = ts;
   return 0;
}

int Index::serialize(const string& path, const string& dstfile)
{
   RWGuard mg(m_MetaLock, RW_READ);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;

   ofstream ofs(dstfile.c_str());
   if (ofs.bad() || ofs.fail())
      return -1;
   int result = serialize(ofs, tree, 1);
   ofs.close();
   return result;
}

int Index::deserialize(const string& path, const string& srcfile,  const Address* addr)
{
   RWGuard mg(m_MetaLock, RW_WRITE);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;

   ifstream ifs(srcfile.c_str());
   if (ifs.bad() || ifs.fail())
      return -1;
   deserialize(ifs, tree, addr);
   ifs.close();
   return 0;
}

int Index::scan(const string& datadir, const string& metadir)
{
   RWGuard mg(m_MetaLock, RW_WRITE);
   STree* tree = lookup(metadir);
   if (tree == NULL)
      return -1;
   scan(datadir, tree);
   return 0;
}

int Index::merge(Metadata* branch, const unsigned int& replica)
{
   RWGuard mg(m_MetaLock, RW_WRITE);
   merge(&m_MetaTree, &((Index*)branch)->m_MetaTree, replica);
   return 0;
}

int Index::substract(const string& path, const Address& addr)
{
   RWGuard mg(m_MetaLock, RW_WRITE);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;
   STree* parent = lookupParent(path);
   // if parent is NULL, it means this is root directory.
   substract(parent, tree, addr);
   return 0;
}

int64_t Index::getTotalDataSize(const string& path)
{
   RWGuard mg(m_MetaLock, RW_READ);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;
   return getTotalDataSize(*tree);
}

int64_t Index::getTotalFileNum(const string& path)
{
   RWGuard mg(m_MetaLock, RW_READ);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;
   return getTotalFileNum(*tree);
}

int Index::collectDataInfo(const string& path, vector<string>& result)
{
   RWGuard mg(m_MetaLock, RW_READ);
   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;
   STree* parent = lookupParent(path);
   // if parent is NULL, it means this is root directory.

   return collectDataInfo(path, parent, tree, result);
}

int Index::checkReplica(const string& path, vector<string>& under, vector<string>& over)
{
   under.clear();
   over.clear();

   RWGuard mg(m_MetaLock, RW_READ);

   STree* tree = lookup(path);
   if (tree == NULL)
      return -1;

   return checkReplica(path, *tree, under, over);
}

int Index::getSlaveMeta(Metadata* branch, const Address& addr)
{
   RWGuard mg(m_MetaLock, RW_READ);

   vector<string> path;
   return getSlaveMeta(m_MetaTree, path, &((Index*)branch)->m_MetaTree, addr);
}

///////////////////////////////////////////////////////////////////////////////////////
// Private functions.

Index::STree* Index::lookup(const vector<string>& dir_vec)
{
   STree* tree = &m_MetaTree;
   map<string, STree>::iterator s;
   for (vector<string>::const_iterator d = dir_vec.begin(); d != dir_vec.end(); ++ d)
   {
      s = tree->m_mDirectory.find(*d);
      if (s == tree->m_mDirectory.end())
         return NULL;

      tree = &(s->second);
   }

   return tree;
}

Index::STree* Index::lookup(const string& path)
{
   vector<string> dir;
   if (parsePath(path, dir) < 0)
      return NULL;
   return lookup(dir);
}

Index::STree* Index::lookupParent(const string& path)
{
   vector<string> dir;
   if (parsePath(path, dir) < 1)
      return NULL;
   dir.pop_back();
   return lookup(dir);
}

Index::STree* Index::create(const string& path)
{
   vector<string> dir;
   if (parsePath(path.c_str(), dir) < 0)
      return NULL;

   STree* tree = &m_MetaTree;
   map<string, STree>::iterator s;
   for (vector<string>::iterator d = dir.begin(); d != dir.end(); ++ d)
   {
      s = tree->m_mDirectory.find(*d);
      if (s == tree->m_mDirectory.end())
      {
         STree t;
         t.m_Node.m_strName = *d;
         t.m_Node.m_bIsDir = true;
         t.m_Node.m_llTimeStamp = time(NULL);
         t.m_Node.m_llSize = 0;
         tree->m_mDirectory[*d] = t;
         s = tree->m_mDirectory.find(*d);
      }
      tree = &(s->second);
   }

   return tree;
}

int Index::serialize(ofstream& ofs, STree* tree, int level)
{
   /*
   DIR_LEVEL FILE_INFO
   1 xxx
   1 xxx
   2 yyy
   2 zzz
   3 ppp
   */

   for (map<string, STree>::iterator i = tree->m_mDirectory.begin(); i != tree->m_mDirectory.end(); ++ i)
   {
      char* buf = NULL;
      if (i->second.m_Node.serialize(buf) >= 0)
         ofs << level << " " << buf << endl;
      delete [] buf;
      serialize(ofs, &i->second, level + 1);
   }

   return ofs.bad() ? -1 : 0;
}

int Index::deserialize(ifstream& ifs, STree* tree, const Address* addr)
{
   vector<string> dirs;
   dirs.resize(1024);
   STree* currdir = tree;
   int currlevel = 1;

   while (!ifs.eof())
   {
      char tmp[4096];
      tmp[4095] = 0;
      char* buf = tmp;

      ifs.getline(buf, 4096);
      int len = strlen(buf);
      if ((len <= 0) || (len >= 4095))
         continue;

      for (int i = 0; i < len; ++ i)
      {
         if (buf[i] == ' ')
         {
            buf[i] = '\0';
            break;
         }
      }
      int level = atoi(buf);

      SNode sn;
      sn.deserialize(buf + strlen(buf) + 1);
      if ((!sn.m_bIsDir) && (NULL != addr))
      {
         sn.m_sLocation.clear();
         sn.m_sLocation.insert(*addr);
      }

      if (level == currlevel)
      {
         currdir->m_mDirectory[sn.m_strName].m_Node = sn;
         dirs[level] = sn.m_strName;
      }
      else if (level == currlevel + 1)
      {
         map<string, STree>::iterator s = currdir->m_mDirectory.find(dirs[currlevel]);
         currdir = &(s->second);
         currlevel = level;

         currdir->m_mDirectory[sn.m_strName].m_Node = sn;
         dirs[level] = sn.m_strName;
      }
      else if (level < currlevel)
      {
         currdir = tree;
         for (int i = 1; i < level; ++ i)
         {
            map<string, STree>::iterator s = currdir->m_mDirectory.find(dirs[i]);
            currdir = &(s->second);
         }
         currlevel = level;

         currdir->m_mDirectory[sn.m_strName].m_Node = sn;
         dirs[level] = sn.m_strName;
      }
   }

   return 0;
}

// Scan a local FS directory and load the information to the Index structure.
int Index::scan(const string& currdir, STree* tree)
{
   vector<SNode> filelist;
   if (LocalFS::list_dir(currdir, filelist) < 0)
      return -1;

   tree->clear();

   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      // skip "." and ".."
      if (i->m_strName.empty() || (i->m_strName == ".") || (i->m_strName == ".."))
         continue;

      // check file name
      bool bad = false;
      for (char *p = (char*)i->m_strName.c_str(), *q = p + i->m_strName.length(); p != q; ++ p)
      {
         if (!m_pbLegalChar[int(*p)])
         {
            bad = true;
            break;
         }
      }
      if (bad)
         continue;

      // skip system file and directory
      // TODO: this should be allowed. put all user data in a special director, and system data to another.
      if (i->m_bIsDir && (i->m_strName.c_str()[0] == '.'))
         continue;

      tree->m_mDirectory[i->m_strName].m_Node = *i;
      map<string, STree>::iterator mi = tree->m_mDirectory.find(i->m_strName);

      if (mi->second.m_Node.m_bIsDir)
         scan(currdir + "/" + mi->first, &mi->second);
   }

   return 0;
}

int Index::merge(STree* currdir, STree* branch, const unsigned int& replica)
{
   vector<string> tbd;

   for (map<string, STree>::iterator i = branch->m_mDirectory.begin(); i != branch->m_mDirectory.end(); ++ i)
   {
      map<string, STree>::iterator s = currdir->m_mDirectory.find(i->first);

      if (s == currdir->m_mDirectory.end())
      {
         currdir->m_mDirectory[i->first] = i->second;
         tbd.push_back(i->first);
      }
      else
      {
         SNode& inode = i->second.m_Node;
         SNode& snode = s->second.m_Node;
         if (inode.m_bIsDir && snode.m_bIsDir)
         {
            // directories with same name
            merge(&s->second, &i->second, replica);

            // if all files have been successfully merged, remove the directory name
            if (i->second.m_mDirectory.empty())
               tbd.push_back(i->first);
         }
         else if (!inode.m_bIsDir && !snode.m_bIsDir &&
                  (inode.m_llSize == snode.m_llSize) &&
                  (inode.m_llTimeStamp == snode.m_llTimeStamp))
                  //&& (snode.m_sLocation.size() < replica))
         {
            // files with same name, size, timestamp
            // and the number of replicas is below the threshold
            for (set<Address, AddrComp>::iterator a = inode.m_sLocation.begin(); a != inode.m_sLocation.end(); ++ a)
               snode.m_sLocation.insert(*a);
            tbd.push_back(i->first);
         }
      }
   }

   for (vector<string>::iterator i = tbd.begin(); i != tbd.end(); ++ i)
      branch->m_mDirectory.erase(*i);

   return 0;
}

int Index::substract(STree* parent, STree* tree, const Address& addr)
{
   if (!tree->m_Node.m_bIsDir)
   {
      tree->m_Node.m_sLocation.erase(addr);
      if (tree->m_Node.m_sLocation.empty())
         parent->m_mDirectory.erase(tree->m_Node.m_strName);
   }
   else
   {
      vector<string> tbd;
      for (map<string, STree>::iterator i = tree->m_mDirectory.begin(); i != tree->m_mDirectory.end(); ++ i)
      {
         if (!i->second.m_Node.m_bIsDir)
         {
            i->second.m_Node.m_sLocation.erase(addr);
            if (i->second.m_Node.m_sLocation.empty())
               tbd.push_back(i->first);
         }
         else
         {
            substract(tree, &i->second, addr);
         }
      }
      for (vector<string>::iterator i = tbd.begin(); i != tbd.end(); ++ i)
         tree->m_mDirectory.erase(*i);
   }

   return 0;
}

int64_t Index::getTotalDataSize(const STree& tree) const
{
   int64_t size = 0;
   if (!tree.m_Node.m_bIsDir)
   {
      size += tree.m_Node.m_llSize;
   }
   else
   {
      for (map<string, STree>::const_iterator i = tree.m_mDirectory.begin(); i != tree.m_mDirectory.end(); ++ i)
         size += getTotalDataSize(i->second);
   }
   return size;
}

int64_t Index::getTotalFileNum(const STree& tree) const
{
   int64_t num = 0;
   if (!tree.m_Node.m_bIsDir)
   {
      ++ num;
   }
   else
   {
      for (map<string, STree>::const_iterator i = tree.m_mDirectory.begin(); i != tree.m_mDirectory.end(); ++ i)
         num += getTotalFileNum(i->second);
   }
   return num;
}

int Index::collectDataInfo(const string& path, const STree* parent, const STree* tree, vector<string>& result) const
{
   const string& filename = tree->m_Node.m_strName;

   if (!tree->m_Node.m_bIsDir)
   {
      // skip system files
      if (filename.c_str()[0] == '.')
         return 0;

      // ignore index file
      int t = filename.length();
      if ((t > 4) && (filename.substr(t - 4, t) == ".idx"))
         return 0;

      string idx = filename + ".idx";
      int64_t rows = -1;
      map<string, STree>::const_iterator s = parent->m_mDirectory.find(idx);
      if (s != parent->m_mDirectory.end())
         rows = s->second.m_Node.m_llSize / 8 - 1;

      stringstream buf;
      buf << path + "/" + filename << " " << tree->m_Node.m_llSize << " " << rows;

      for (set<Address, AddrComp>::iterator i = tree->m_Node.m_sLocation.begin(); i != tree->m_Node.m_sLocation.end(); ++ i)
         buf << " " << i->m_strIP << " " << i->m_iPort;

      result.push_back(buf.str());
   }
   else
   {
      for (map<string, STree>::const_iterator i = tree->m_mDirectory.begin(); i != tree->m_mDirectory.end(); ++ i)
         collectDataInfo(path + "/" + filename, tree, &i->second, result);
   }

   return result.size();
}

int Index::checkReplica(const string& path, const STree& tree, vector<string>& under, vector<string>& over) const
{
   string abs_path = path;
   if (path == "/")
      abs_path += tree.m_Node.m_strName;
   else
      abs_path += "/" + tree.m_Node.m_strName;

   if ((!tree.m_Node.m_bIsDir) || (tree.m_mDirectory.find(NOSPLIT) != tree.m_mDirectory.end()))
   {
      // replicate a file according to the number of specified replicas
      // or if this is a directory and it contains a file called NOSPLIT,
      // the whole directory will be replicated together.

      unsigned int curr_rep_num = 0;
      unsigned int target_rep_num = 0;
      map<string, STree>::const_iterator ns = tree.m_mDirectory.find(NOSPLIT);
      if (ns != tree.m_mDirectory.end())
      {
         curr_rep_num = ns->second.m_Node.m_sLocation.size();
         target_rep_num = ns->second.m_Node.m_iReplicaNum;
      }
      else
      {
         curr_rep_num = tree.m_Node.m_sLocation.size();
         target_rep_num = tree.m_Node.m_iReplicaNum;
      }

      if (curr_rep_num < target_rep_num)
         under.push_back(abs_path);
      else if (curr_rep_num > target_rep_num)
         over.push_back(abs_path);

      return 0;
   }

   for (map<string, STree>::const_iterator i = tree.m_mDirectory.begin(); i != tree.m_mDirectory.end(); ++ i)
      checkReplica(abs_path, i->second, under, over);

   return 0;
}

int Index::list_r(const STree& tree, const string& path, vector<string>& filelist) const
{
   if (!tree.m_Node.m_bIsDir ||
       (tree.m_mDirectory.find(NOSPLIT) != tree.m_mDirectory.end())) 
   {
      filelist.push_back(path);
      return filelist.size();
   }

   for (map<string, STree>::const_iterator i = tree.m_mDirectory.begin(); i != tree.m_mDirectory.end(); ++ i)
   {
      list_r(i->second, path + "/" + i->first, filelist);
   }

   return filelist.size();
}

int Index::getSlaveMeta(const STree& tree, const vector<string>& path, STree* target, const Address& addr) const
{
   if (tree.m_Node.m_sLocation.find(addr) != tree.m_Node.m_sLocation.end())
   {
      STree* sub_target = target;
      for (vector<string>::const_iterator d = path.begin(); d != path.end(); ++ d)
      {
         if (sub_target->m_mDirectory.find(*d) == sub_target->m_mDirectory.end())
         {
            STree t;
            t.m_Node.m_strName = *d;
            t.m_Node.m_bIsDir = true;
            t.m_Node.m_llTimeStamp = time(NULL);
            t.m_Node.m_llSize = 0;
            sub_target->m_mDirectory[*d] = t;
            sub_target = &(sub_target->m_mDirectory.find(*d)->second);
         }
      }
   }

   vector<string> new_path = path;
   new_path.push_back(tree.m_Node.m_strName);
   for (map<string, STree>::const_iterator i = tree.m_mDirectory.begin(); i != tree.m_mDirectory.end(); ++ i)
      getSlaveMeta(i->second, new_path, target, addr);

   return 0;
}

void Index::refreshRepSetting(const string& path, int default_num, int default_dist,
                              map<string, int>& rep_num, map<string, int>& rep_dist,
                              map<string, vector<int> >& restrict_loc)
{
   RWGuard mg(m_MetaLock, RW_WRITE);
   STree* tree = lookup(path);
   if (tree == NULL)
      return;
   refreshRepSetting(path, tree, default_num, default_dist, rep_num, rep_dist, restrict_loc);
}

int Index::refreshRepSetting(const string& path, STree* tree, int default_num, int default_dist,
                             map<string, int>& rep_num, map<string, int>& rep_dist, map<string, vector<int> >& restrict_loc)
{
   //TODO: use wildcard match each level of dir, instead of contain()

   string abs_path = path;
   if (path == "/")
      abs_path += tree->m_Node.m_strName;
   else
      abs_path += "/" + tree->m_Node.m_strName;

   // set replication factor
   tree->m_Node.m_iReplicaNum = default_num;
   for (map<string, int>::const_iterator rn = rep_num.begin(); rn != rep_num.end(); ++ rn)
   {
      if (WildCard::contain(rn->first, abs_path))
      {
         tree->m_Node.m_iReplicaNum = rn->second;
         break;
      }
   }

   // set replication distance
   tree->m_Node.m_iReplicaDist = default_dist;
   for (map<string, int>::const_iterator rd = rep_dist.begin(); rd != rep_dist.end(); ++ rd)
   {
      if (WildCard::contain(rd->first, abs_path))
      {
         tree->m_Node.m_iReplicaDist = rd->second;
         break;
       }
   }

   // set restricted location
   tree->m_Node.m_viRestrictedLoc.clear();
   for (map<string, vector<int> >::const_iterator rl = restrict_loc.begin(); rl != restrict_loc.end(); ++ rl)
   {
      if (WildCard::contain(rl->first, abs_path))
      {
         tree->m_Node.m_viRestrictedLoc = rl->second;
         break;
      }
   }

   for (map<string, STree>::iterator i = tree->m_mDirectory.begin(); i != tree->m_mDirectory.end(); ++i)
      refreshRepSetting(abs_path, &i->second, default_num, default_dist, rep_num, rep_dist, restrict_loc);

   return 0;
}
