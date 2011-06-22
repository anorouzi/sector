/*****************************************************************************
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
   Yunhong Gu, last updated 03/17/2011
*****************************************************************************/

#include <assert.h>
#include <string.h>

#include <common.h>
#include "fscache.h"

using namespace std;

Cache::Cache():
m_llCacheSize(0),
m_iBlockNum(0),
m_llMaxCacheSize(10000000),
m_llMaxCacheTime(10000000),
m_iMaxCacheBlocks(4096),
m_iBlockUnitSize(1000000)
{
   CGuard::createMutex(m_Lock);
}

Cache::~Cache()
{
   CGuard::releaseMutex(m_Lock);
}

int Cache::setCacheBlockSize(const int size)
{
   CGuard sg(m_Lock);

   // Cannot change block size if there is already data in the cache.
   if (m_llCacheSize > 0)
      return -1;

   m_iBlockUnitSize = size;
   return 0;
}

int Cache::setMaxCacheSize(const int64_t& ms)
{
   CGuard sg(m_Lock);
   m_llMaxCacheSize = ms;
   return 0;
}

int Cache::setMaxCacheTime(const int64_t& mt)
{
   CGuard sg(m_Lock);
   m_llMaxCacheTime = mt;
   return 0;
}

int Cache::setMaxCacheBlocks(const int num)
{
   CGuard sg(m_Lock);
   m_iMaxCacheBlocks = num;
   return 0;
}

void Cache::update(const string& path, const int64_t& ts, const int64_t& size, bool first)
{
   CGuard sg(m_Lock);

   InfoBlockMap::iterator s = m_mOpenedFiles.find(path);

   if (s == m_mOpenedFiles.end())
   {
      InfoBlock r;
      r.m_iCount = 1;
      r.m_bChange = false;
      r.m_llTimeStamp = ts;
      r.m_llSize = size;
      r.m_llLastAccessTime = CTimer::getTime() / 1000000;
      m_mOpenedFiles[path] = r;
      return;
   }

   if ((s->second.m_llTimeStamp != ts) || (s->second.m_llSize != size))
   {
      s->second.m_bChange = true;
      s->second.m_llTimeStamp = ts;
      s->second.m_llSize = size;
      s->second.m_llLastAccessTime = CTimer::getTime() / 1000000;
   }

   // Increase reference count.
   if (first)
      s->second.m_iCount ++;
}

void Cache::remove(const string& path)
{
   CGuard sg(m_Lock);

   map<string, InfoBlock>::iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return;

   // Remove the file information when its reference count becomes 0.
   if (-- s->second.m_iCount == 0)
      m_mOpenedFiles.erase(s);

   // Note that we do not remove the data cache even if the file is closed,
   // in case it may be opened again in the near future.
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

   InfoBlockMap::iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return -1;
   s->second.m_llLastAccessTime = CTimer::getTime() / 1000000;

   int first_block;
   int block_num;
   parseIndexOffset(offset, size, first_block, block_num);

   // check if the cache already exists or overlap with existing blocks
   FileCacheMap::iterator c = m_mFileCache.find(path);
   if (c != m_mFileCache.end())
   {
      // If this block overlapps with existing blocks, delete existing blocks.
      for (int i = first_block, n = first_block + block_num; i < n; ++ i)
      {
         BlockIndexMap::iterator it = c->second.find(i);
         if (it != c->second.end())
            releaseBlock(it->second);
      }
   }

   // Check again as the blocks may be released just now.
   if (m_mFileCache.find(path) == m_mFileCache.end())
   {
      // This is the first cache block for this file.
      m_mFileCache[path].clear();
      c = m_mFileCache.find(path);
   }

   CacheBlock* cb = new CacheBlock;
   cb->m_strFile = path;
   cb->m_llOffset = offset;
   cb->m_llSize = size;
   cb->m_llCreateTime = s->second.m_llLastAccessTime;
   cb->m_llLastAccessTime = s->second.m_llLastAccessTime;
   cb->m_pcBlock = block;
   cb->m_bWrite = write;

   // insert at the end of the list, newest block
   CacheBlockIter it = m_lCacheBlocks.insert(m_lCacheBlocks.end(), cb);

   // update per-file index
   for (int i = first_block, n = first_block + block_num; i < n; ++ i)
      c->second[i] = it;

   m_llCacheSize += cb->m_llSize;
   ++ m_iBlockNum;

   // check and remove old caches to limit memory usage
   shrink();

   return 0;
}

int64_t Cache::read(const string& path, char* buf, const int64_t& offset, const int64_t& size)
{
   CGuard sg(m_Lock);

   InfoBlockMap::const_iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return -1;

   FileCacheMap::iterator c = m_mFileCache.find(path);
   if (c == m_mFileCache.end())
      return 0;

   int first_block;
   int block_num;
   parseIndexOffset(offset, size, first_block, block_num);

   BlockIndexMap::iterator block = c->second.find(first_block);
   if (block == c->second.end())
      return 0;

   CacheBlock* cb = *block->second;

   // We only read full size block.
   // TODO: allow partial read.
   if (cb->m_llOffset + cb->m_llSize < offset + size)
      return 0;

   memcpy(buf, cb->m_pcBlock + offset - cb->m_llOffset, size);

   // Update the block by moving it to the tail of the cache list
   if (m_lCacheBlocks.size() > 1)
   {
      m_lCacheBlocks.erase(block->second);
      CacheBlockIter it = m_lCacheBlocks.insert(m_lCacheBlocks.end(), cb);

      // update per-file index
      for (int i = first_block, n = first_block + block_num; i < n; ++ i)
         c->second[i] = it;
   }

   return size;
}

char* Cache::retrieve(const std::string& path, const int64_t& offset, const int64_t& size)
{
   CGuard sg(m_Lock);

   InfoBlockMap::const_iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return NULL;

   FileCacheMap::iterator c = m_mFileCache.find(path);
   if (c == m_mFileCache.end())
      return NULL;

   int first_block;
   int block_num;
   parseIndexOffset(offset, size, first_block, block_num);

   BlockIndexMap::iterator block = c->second.find(first_block);
   if (block == c->second.end())
      return NULL;

   CacheBlock* cb = *block->second;
   if ((cb->m_llOffset != offset) || (cb->m_llSize != size))
      return NULL;

   return cb->m_pcBlock;
}

void Cache::shrink()
{
   // The head node on the cache list is the oldest block, remove it to reduce cache size.

   while ((m_llCacheSize > m_llMaxCacheSize) || (m_iBlockNum > m_iMaxCacheBlocks))
   {
      if (m_lCacheBlocks.empty())
         return;

      CacheBlock* cb = m_lCacheBlocks.front();
      if (cb->m_bWrite)
         return;

      int first_block;
      int block_num;
      parseIndexOffset(cb->m_llOffset, cb->m_llSize, first_block, block_num);

      FileCacheMap::iterator c = m_mFileCache.find(cb->m_strFile);
      assert(c != m_mFileCache.end());

      for (int i = first_block, n = first_block + block_num; i < n; ++ i)
         c->second.erase(i);

      m_llCacheSize -= cb->m_llSize;
      m_iBlockNum --;

      m_lCacheBlocks.pop_front();
      delete [] cb->m_pcBlock;
      delete cb;
   }
}

int Cache::clearWrite(const string& path, const int64_t& offset, const int64_t& size)
{
   CGuard sg(m_Lock);

   InfoBlockMap::const_iterator s = m_mOpenedFiles.find(path);
   if (s == m_mOpenedFiles.end())
      return -1;

   FileCacheMap::iterator c = m_mFileCache.find(path);
   if (c == m_mFileCache.end())
      return 0;

   int first_block;
   int block_num;
   parseIndexOffset(offset, size, first_block, block_num);

   BlockIndexMap::iterator block = c->second.find(first_block);
   assert(block != c->second.end());

   (*block->second)->m_bWrite = false;

   // Cache may not be reduced by other operations if app does write only, so we try to reduce cache block here.
   while ((m_llCacheSize > m_llMaxCacheSize) || (m_iBlockNum > m_iMaxCacheBlocks))
      shrink();

   return 0;
}

void Cache::parseIndexOffset(const int64_t& offset, const int64_t& size, int& index_off, int& block_num)
{
   index_off = offset / m_iBlockUnitSize;
   if ((offset != 0) && ((offset % m_iBlockUnitSize) == 0))
      index_off ++;

   block_num = (offset + size) / m_iBlockUnitSize - index_off + 1;
}

void Cache::releaseBlock(CacheBlockIter& it)
{
   CacheBlock* cb = *it;

   int first_block;
   int block_num;
   parseIndexOffset(cb->m_llOffset, cb->m_llSize, first_block, block_num);

   FileCacheMap::iterator c = m_mFileCache.find(cb->m_strFile);
   assert(c != m_mFileCache.end());

   for (int i = first_block, n = first_block + block_num; i < n; ++ i)
      c->second.erase(i);
   if (c->second.empty())
      m_mFileCache.erase(c);

   m_llCacheSize -= cb->m_llSize;
   m_iBlockNum --;

   m_lCacheBlocks.erase(it);
   delete [] cb->m_pcBlock;
   delete cb;
}
