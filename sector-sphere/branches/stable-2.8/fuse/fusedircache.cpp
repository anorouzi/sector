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

void DirCache::clear_old() {
    
    Lock l(mutex);
    time_t tsNow = time(NULL);

    CacheList::iterator it = cache.begin(), end = cache.end();
    for (; it != end; ++it) {
        if (it->ts + TIME_OUT < tsNow) {
            cache.erase(it, end);
            return;
        }
    }
}

void DirCache::clear() {

    Lock l(mutex);
    cache.clear();
}

void DirCache::add(const std::string& path, const std::vector<SNode>& filelist) {
//    log << "cache add " << path <<  " start ======================= " << std::endl;

    Lock l(mutex);
//    log << "cache add " << path <<  " got lock" << std::endl;
    time_t tsNow = time(NULL);
    CacheList::iterator it = cache.begin(), end = cache.end();
//    log << "got iterator" << std::endl;
    while( it != end ) {
//        log << "in iterator" << std::endl;
        if (it->ts + TIME_OUT < tsNow) {
//            log << "cache erase timeout " << it->path << std::endl;
            cache.erase(it, end);
            break;
        }
        else if (it->path == path) {
//            log << "cache erase replace " << it->path << std::endl;
            it = cache.erase(it);
        }
        ++it;
    }

    CacheRec rec = { path, tsNow, filelist };
    cache.push_front(rec);
//    log << "cache add complete " << path << std::endl;
}

int DirCache::get(const std::string& path, Sector& sectorClient, SNode& node) {
// return codes : 1 - cache miss, 0 - cache hit, <0 - sector error
//    assert(!path.empty());
//    log << "get " << path << " start ======================= " << std::endl;

    if (path == "/") {
//      log << "get root" << std::endl;
      if (rootNode.m_strName.empty() ) {
//        log << "get root init" << std::endl;
        int r = sectorClient.stat("/", rootNode);
        if (r < 0) return r;
      }
      node = rootNode;
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

    CacheList::iterator end;
    std::vector<SNode>* pfilelist = NULL;
    {
        Lock lock(mutex);
        end = cache.end();
        for (CacheList::iterator it = cache.begin(); it != end; ++it) {
            if (it->ts + TIME_OUT < tsNow) {
//                log << "get " << path << " cache erase to end " << it->path << std::endl;
                cache.erase(it, end);
                break;
            } else if (it->path == dirpath) {
//               log << "get " << path << " dir cache hit " << it->path << std::endl;
               pfilelist = &(it->filelist);
            }
        }
    }

    std::vector<SNode> filelist;
    if (  pfilelist == NULL ) {
//      log << "get " << path << " miss" << std::endl;
//      log << "lastUnresolvedStatPath " << lastUnresolvedStatPath << " lastUnresolvedStatPathTs "
//          << lastUnresolvedStatPathTs << " tsNow " << tsNow << std::endl;
// No need to have lock around timeout comparison, as worse that can happen - 
// we will do extra ls instead of stat
      if (lastUnresolvedStatPath == dirpath && lastUnresolvedStatPathTs + TIME_OUT >= tsNow) {
//        log << "get " << path << " repeated miss - get dir" << std::endl;
        int r = sectorClient.list(dirpath, filelist);
        if (r < 0) return r;

        pfilelist = &filelist;
        add(dirpath, filelist);
      } else {
//        log << "get " << path << " first miss - return miss" << std::endl;
        {
          Lock lock (mutex);
          lastUnresolvedStatPath = dirpath;
          lastUnresolvedStatPathTs = tsNow;
        }
        return 1;
      } 
    }
//    log << "get " << path << " looking through cache" << std::endl;
    for (std::vector<SNode>::iterator i = pfilelist->begin(); i != pfilelist->end(); ++ i)
    {
//      log << "checking cache entry " << i->m_strName << std::endl;
      if (i->m_strName == filename)
      {
//       log << "get " << path << " return hit" << std::endl;
       node = *i;
       return 0;
      }
    }

//    log << "get " << path << " return miss after dir hit" << std::endl;

    return 1;

}

