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
   Yunhong Gu, last updated 04/23/2010
*****************************************************************************/

#ifndef __SECTOR_FS_CACHE_H__
#define __SECTOR_FS_CACHE_H__

#include <index.h>
#include <string>
#include <map>
#include <list>

#ifndef WIN32 // <slr>
   #include <pthread.h>
#endif

struct InfoBlock
{
   int m_iCount;		// number of reference
   int m_bChange;		// if the file has been changed
   int64_t m_llTimeStamp;	// time stamp
   int64_t m_llSize;		// file size
   int64_t m_llLastAccessTime;	// last access time
};

struct CacheBlock
{
   int64_t m_llOffset;                  // cache block offset
   int64_t m_llSize;                    // cache size
   int64_t m_llCreateTime;              // cache creation time
   int64_t m_llLastAccessTime;          // cache last access time
   int m_iAccessCount;                  // number of accesses
   char* m_pcBlock;                     // cache data
   bool m_bWrite;			// if this block is being written
};

class Cache
{
public:
   Cache();
   ~Cache();

   int setMaxCacheSize(const int64_t ms);
   int setMaxCacheTime(const int64_t mt);
   int setMaxCacheBlocks(const int num);

public:
   void update(const std::string& path, const int64_t& ts, const int64_t& size, bool first = false);
   void remove(const std::string& path);

public:
   int stat(const std::string& path, SNode& attr);

public:
   int insert(char* block, const std::string& path, const int64_t& offset, const int64_t& size, const bool& write = false);
   int64_t read(const std::string& path, char* buf, const int64_t& offset, const int64_t& size);
   char* retrieve(const std::string& path, const int64_t& offset, const int64_t& size);
   int clearWrite(const std::string& path, const int64_t& offset, const int64_t& size);

private:
   int shrink();

private:
   std::map<std::string, InfoBlock> m_mOpenedFiles;
   std::map<std::string, std::list<CacheBlock> > m_mCacheBlocks;
   int64_t m_llCacheSize;               // total size of cache
   int m_iBlockNum;                     // total number of cache blocks
   int64_t m_llMaxCacheSize;            // maximum size of cache allowed
   int64_t m_llMaxCacheTime;            // maximum time to stay in the cache without IO
   int m_iMaxCacheBlocks;               // maximum number of cache blocks

   CMutex m_Lock;
};


#endif
