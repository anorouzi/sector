//#include <assert.h>
#include "fusedircache.h"

DirCache* DirCache::inst = NULL;

DirCache::DirCache()
//    : log("/tmp/fuse-dircache-debug.log", std::ios::app)
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);

    pthread_mutex_init(&mutex, &attr);

//    log << "DirCache started" << std::endl;
}

DirCache::~DirCache()
{
    pthread_mutex_destroy(&mutex);
}

void DirCache::clear_cache() {
//    log << "cache clear" << std::endl;
    Lock l(mutex);
    cache.clear();
    lastUnresolvedStatPathTs = 0;
}

void DirCache::clearLastUnresolvedStatLocal(){
//    log << "Cache clear last unresoved stat" << std::endl;
    lastUnresolvedStatPathTs = 0;
}

void DirCache::add(const std::string& path, const std::vector<SNode>& filelist) {
//    log << "cache add " << path <<  " start ======================= " << std::endl;

    Lock l(mutex);
    time_t tsNow = time(0);
//    log << "Removing timed out entries with ts " << tsNow << " TIME_OUT " << TIME_OUT << std::endl;
    CacheMap::iterator nit = cache.begin();
//    log << "got iterator" << std::endl;
    while( nit != cache.end() ) {
//        log << "in iterator " << nit->second.path << std::endl;
        if (nit->second.ts + TIME_OUT < tsNow) {
//            log << "cache erase timeout " << nit->second.path << std::endl;
            cache.erase(nit++);
        } else
        {
          ++nit;
        }
    }
//    log << "Adding new entry ts " << tsNow << std::endl;
    CacheRec& rec = cache[path];
    rec.path = path;
    rec.ts = tsNow;
// clear map if record already present
    rec.filemap.clear();
    
    std::vector<SNode>::const_iterator fit = filelist.begin(), fend = filelist.end();
    while (fit != fend)
    {
      rec.filemap[fit->m_strName] = *fit;
      ++fit;
    }

//    log << "cache add complete " << path << std::endl;
}

int DirCache::get(const std::string& path, Sector& sectorClient, SNode& node) {
// return codes : 1 - cache miss, 0 - cache hit, <0 - sector error
//    assert(!path.empty());
//    log << "get " << path << " start ======================= " << std::endl;
//return 1;
    if (path == "/") {
//      log << "get root" << std::endl;
      if (rootNode.m_strName.empty() ) {
//        log << "get root init" << std::endl;
        int r = sectorClient.stat("/", rootNode);
        if (r < 0) return r;
      }
      node = rootNode;
//      log << "root node" << std::endl;
      return 0;
    }
    time_t tsNow = time(0);
    std::string::size_type pos = path.find_last_of('/');

    std::string dirpath;
    std::string filename;
    if( pos == std::string::npos )
    {
//        log << "pos 0 or npos" << std::endl;
//        dirpath =  "/";
        filename = path;
    } else {
//        log << "pos " << pos << std::endl;
        if (pos == 0 )
          dirpath = "/";
        else
          dirpath=path.substr(0, pos);
        filename = path.substr(pos + 1);
    }

//    log << "get " << path << " dirpath " << dirpath << " filename " << filename << std::endl;

    {
        Lock lock(mutex);
      
        CacheMap::iterator it = cache.find(dirpath);
        if (it != cache.end())
        {
//          log << "get " << path << " cache hit for " << it->first << std::endl;
          if (it->second.ts + TIME_OUT < tsNow)
          {
//           log << "get " << path << " cache hit but timeout ts " << it->second.ts << " tsNow " << tsNow << " TIME_OUT " << TIME_OUT << " - erasing " << it->second.path << std::endl;
            cache.erase(it);
          }
          FileMap::iterator fit = it->second.filemap.find(filename);
          if (fit != it->second.filemap.end())
          {
            node = fit->second;
//           log << "get " << path << " return hit, file size " << node.m_llSize << std::endl;
            return 0;
          }
        }
     }

//      log << "get " << path << " miss" << std::endl;
//      log << "lastUnresolvedStatPath " << lastUnresolvedStatPath << " lastUnresolvedStatPathTs "
//          << lastUnresolvedStatPathTs << " tsNow " << tsNow << std::endl;
// No need to have lock around timeout comparison, as worse that can happen - 
// we will do extra ls instead of stat
    if (lastUnresolvedStatPathTs + TIME_OUT >= tsNow && lastUnresolvedStatPath == dirpath) {
//        log << "get " << path << " repeated miss - get dir " << dirpath << std::endl;
      std::vector<SNode> pfilelist;
      int r = sectorClient.list(dirpath, pfilelist);
//      log << "get " << path << " dir done " << std::endl;
      if (r < 0) return r;
      for (std::vector<SNode>::iterator i = pfilelist.begin(); i != pfilelist.end(); ++ i)
      {
//                log << "checking cache entry " << i->m_strName << " against " << filename << std::endl;
          if (i->m_strName == filename)
          {
//                   log << "get " << path << " return hit after dir read, file size " << i->m_llSize << std::endl;
              node = *i;
              add(dirpath, pfilelist);
              return 0;
          }
      }
// It was questionable add -  if file not there, most probably it is check before creating new
//      add(dirpath, pfilelist);
    } else {
//        log << "get " << path << " first miss - return miss" << std::endl;
      {
        Lock lock (mutex);
        lastUnresolvedStatPath = dirpath;
        lastUnresolvedStatPathTs = tsNow;
      }
      return 1;
    } 

//    log << "get " << path << " return miss after dir hit" << std::endl;

    return 1;

}

