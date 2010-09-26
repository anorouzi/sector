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
   Yunhong Gu, last updated 08/23/2010
*****************************************************************************/

#include <string.h>
#include <common.h>
#include "fscache.h"

using namespace std;

Cache::Cache():
m_llCacheSize(0),
m_iBlockNum(0),
m_llMaxCacheSize(10000000),
m_llMaxCacheTime(10000000),
m_iMaxCacheBlocks(32)
{



}

Cache::~Cache()
{



}

int Cache::setMaxCacheSize(const int64_t ms)
{
   m_llMaxCacheSize = ms;
   return 0;
}

int Cache::setMaxCacheTime(const int64_t mt)
{
   m_llMaxCacheTime = mt;
   return 0;
}

int Cache::setMaxCacheBlocks(const int num)
{
   m_iMaxCacheBlocks = num;
   return 0;
}

void Cache::update(const string& path, const int64_t& ts, const int64_t& size, bool first)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
   {
      InfoBlock r;
      r.m_iCount = 1;
      r.m_bChange = false;
      r.m_llTimeStamp = ts;
      r.m_llSize = size;
      r.m_llLastAccessTime = CTimer::getTime();
      m_mOpenedFiles[path] = r;

      return;
   }

   if ((s->second.m_llTimeStamp != ts) || (s->second.m_llSize != size))
   {
      s->second.m_bChange = true;
      s->second.m_llTimeStamp = ts;
      s->second.m_llSize = size;
      s->second.m_llLastAccessTime = CTimer::getTime();
   }

   if (first)
      s->second.m_iCount ++;
}

void Cache::remove(const string& path)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
      return;

   if (-- s->second.m_iCount == 0)
   {
      map<string, list<CacheBlock> > ::iterator c = m_mCacheBlocks.find(path);
      if (c != m_mCacheBlocks.end())
      {
         for (list<CacheBlock>::iterator i = c->second.begin(); i != c->second.end(); ++ i)
         {
            delete [] i->m_pcBlock;
            i->m_pcBlock = NULL;
            m_llCacheSize -= i->m_llSize;
            -- m_iBlockNum;
         }

         m_mCacheBlocks.erase(c);
      }

      m_mOpenedFiles.erase(s);
   }
}

int Cache::stat(const string& path, SNode& attr)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
      return -1;

   if (!s->second.m_bChange)
      return 0;

   attr.m_llTimeStamp = s->second.m_llTimeStamp;
   attr.m_llSize = s->second.m_llSize;

   return 1;
}

int Cache::insert(char* block, const string& path, const int64_t& offset, const int64_t& size, const bool& write)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return -1;

   s->second.m_llLastAccessTime = CTimer::getTime();

   CacheBlock cb;
   cb.m_llOffset = offset;
   cb.m_llSize = size;
   cb.m_llCreateTime = CTimer::getTime();
   cb.m_llLastAccessTime = CTimer::getTime();
   cb.m_pcBlock = block;
   cb.m_bWrite = write;

   map<string, list<CacheBlock> >::iterator c = m_mCacheBlocks.find(path);

   try
   {
      if (c == m_mCacheBlocks.end())
         m_mCacheBlocks[path].push_front(cb);
      else
         c->second.push_front(cb);
   }
   catch (...)
   {
      return -1;
   }

   m_llCacheSize += cb.m_llSize;
   ++ m_iBlockNum;

   if (write)
   {
      //write invalidates all caches overlap with this block
      // TODO: optimize this
      c = m_mCacheBlocks.find(path);
      if (c != m_mCacheBlocks.end())
      {
         for (list<CacheBlock>::iterator i = c->second.begin(); i != c->second.end();)
         {
            if ((i->m_llOffset <= offset) && (i->m_llOffset + i->m_llSize > offset) && !i->m_bWrite)
            {
               list<CacheBlock>::iterator j = i;
               ++ i;
               delete [] j->m_pcBlock;
               j->m_pcBlock = NULL;
               m_llCacheSize -= j->m_llSize;
               -- m_iBlockNum;
               c->second.erase(j);
            }
            else
            {
               ++ i;
            }
         }
      }
   }

   // remove old caches to limit memory usage
   shrink();

   return 0;
}

int64_t Cache::read(const string& path, char* buf, const int64_t& offset, const int64_t& size)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return -1;

   map<string, list<CacheBlock> >::iterator c = m_mCacheBlocks.find(path);
   if (c == m_mCacheBlocks.end())
      return 0;

   for (list<CacheBlock>::iterator i = c->second.begin(); i != c->second.end(); ++ i)
   {
      // this condition can be improved to provide finer granularity
      if ((offset >= i->m_llOffset) && (i->m_llSize - (offset - i->m_llOffset) >= size))
      {
         memcpy(buf, i->m_pcBlock + offset - i->m_llOffset, int(size));
         i->m_llLastAccessTime = CTimer::getTime();
         // update the file's last access time; it must equal to the block's last access time
         s->second.m_llLastAccessTime = i->m_llLastAccessTime;
         return static_cast<int>(size);
      }

      // search should not go further if an overlap block is found, due to possible write conflict (multiple writes on the same block)
      if ((i->m_llOffset <= offset) && (i->m_llOffset + i->m_llSize > offset))
         break;
   }

   return 0;
}

int Cache::shrink()
{
   if ((m_llCacheSize < m_llMaxCacheSize) && (m_iBlockNum < m_iMaxCacheBlocks))
      return 0;

   string last_file = "";
   int64_t latest_time = CTimer::getTime();

   // find the file with the earliest last access time
   for (map<string, InfoBlock>::iterator i = m_mOpenedFiles.begin(); i != m_mOpenedFiles.end(); ++ i)
   {
      if (i->second.m_llLastAccessTime < latest_time)
      {
         map<string, list<CacheBlock> >::iterator c = m_mCacheBlocks.find(i->first);
         if ((c != m_mCacheBlocks.end()) && !c->second.empty())
         {
            last_file = i->first;
            latest_time = i->second.m_llLastAccessTime;
         }
      }
   }

   // find the block with the earliest lass access time
   map<string, list<CacheBlock> >::iterator c = m_mCacheBlocks.find(last_file);

   if (c == m_mCacheBlocks.end())
   {
      // this should not happen
      return 0;
   }

   latest_time = CTimer::getTime();
   list<CacheBlock>::iterator d = c->second.end();
   for (list<CacheBlock>::iterator i = c->second.begin(); i != c->second.end(); ++ i)
   {
      // write cache MUST NOT be removed until the write is cleared
      if ((i->m_llLastAccessTime < latest_time) && !i->m_bWrite)
      {
         latest_time = i->m_llLastAccessTime;
         d = i;
      }
   }

   if (d == c->second.end())
      return 0;

   delete [] d->m_pcBlock;
   d->m_pcBlock = NULL;
   m_llCacheSize -= d->m_llSize;
   -- m_iBlockNum;
   c->second.erase(d);

   return 0;
}

char* Cache::retrieve(const string& path, const int64_t& offset, const int64_t& size)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return NULL;

   map<string, list<CacheBlock> >::iterator c = m_mCacheBlocks.find(path);
   if (c == m_mCacheBlocks.end())
      return NULL;

   for (list<CacheBlock>::iterator i = c->second.begin(); i != c->second.end(); ++ i)
   {
      if ((offset == i->m_llOffset) && (size == i->m_llSize))
         return i->m_pcBlock;
   }

   return NULL;
}

int Cache::clearWrite(const string& path, const int64_t& offset, const int64_t& size)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return 0;

   map<string, list<CacheBlock> >::iterator c = m_mCacheBlocks.find(path);
   if (c == m_mCacheBlocks.end())
      return 0;

   for (list<CacheBlock>::iterator i = c->second.begin(); i != c->second.end(); ++ i)
   {
      if ((offset == i->m_llOffset) && (size == i->m_llSize))
      {
         i->m_bWrite = false;
         return 0;
      }
   }

   return 0;
}

