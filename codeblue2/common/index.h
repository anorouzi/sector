/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 07/02/2008
*****************************************************************************/


#ifndef __SECTOR_INDEX_H__
#define __SECTOR_INDEX_H__

#include <string>
#include <vector>
#include <map>
#include <set>
#include <fstream>

struct Address
{
   std::string m_strIP;
   std::string m_strPublicIP;
   unsigned short int m_iPort;
};

struct AddrComp
{
   bool operator()(const Address& a1, const Address& a2) const
   {
      if (a1.m_strIP == a2.m_strIP)
         return a1.m_iPort < a2.m_iPort;
      return a1.m_strIP < a2.m_strIP;
   }
};

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
   Index() {}
   ~Index() {}

public:
   int list(const char* path, std::vector<std::string>& filelist);
   int lookup(const char* path, SNode& attr);
   int lookup(const char* path, std::set<Address, AddrComp>& addr);

public:
   int create(const char* path, bool isdir = false);
   int move(const char* oldpath, const char* newpath);
   int remove(const char* path, bool recursive = false);
   int addCopy(const char* path, const Address& loc);
   int eraseCopy(const char* path, const Address& loc);
   int update(const char* fileinfo, const Address& addr, const int& type, std::set<Address, AddrComp>& tbr);

public:
   int lock(const char* path, int mode);
   int unlock(const char* path, int mode);
 
public:
   static int serialize(std::ofstream& ofs, std::map<std::string, SNode>& currdir, int level);
   static int deserialize(std::ifstream& ifs, std::map<std::string, SNode>& currdir, const Address& addr);
   static int scan(const std::string& currdir, std::map<std::string, SNode>& metadata);
   static int merge(std::map<std::string, SNode>& currdir, std::map<std::string, SNode>& branch);
   static int substract(std::map<std::string, SNode>& currdir, const Address& addr);

   static int64_t getTotalDataSize(std::map<std::string, SNode>& currdir);
   static int64_t getTotalFileNum(std::map<std::string, SNode>& currdir);

   static int parsePath(const char* path, std::vector<std::string>& result);

private:
   std::map<std::string, SNode> m_mDirectory;
};

#endif
