#include "fusedircache.h"

DirCache* DirCache::inst = NULL;

DirCache::DirCache()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);

    pthread_mutex_init(&mutex, &attr);
}

DirCache::~DirCache()
{
    pthread_mutex_destroy(&mutex);
}

// Must be called prior to multithreaded use because we do not acquire lock
int DirCache::init_root( Sector& sectorClient ) {
    return sectorClient.stat("/", rootNode);
}

void DirCache::clear_cache() {
    Lock l(mutex);
    cache.clear();
    lastUnresolvedStatPathTs = 0;
}

void DirCache::clearLastUnresolvedStatLocal(){
    lastUnresolvedStatPathTs = 0;
}

void DirCache::add(const std::string& path, const std::vector<SNode>& filelist) {

    Lock l(mutex);
    time_t tsNow = time(0);
    CacheMap::iterator nit = cache.begin();
    while( nit != cache.end() ) {
        if (nit->second.ts + TIME_OUT < tsNow) {
            cache.erase(nit++);
        } else
        {
          ++nit;
        }
    }
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

}

int DirCache::get(const std::string& path, Sector& sectorClient, SNode& node) {
// return codes : 1 - cache miss, 0 - cache hit, <0 - sector error
    if (path == "/") {
      node = rootNode;
      return 0;
    }
    time_t tsNow = time(0);
    std::string::size_type pos = path.find_last_of('/');

    std::string dirpath;
    std::string filename;
    if( pos == std::string::npos )
    {
        filename = path;
    } else {
        if (pos == 0 )
          dirpath = "/";
        else
          dirpath=path.substr(0, pos);
        filename = path.substr(pos + 1);
    }

    {
        Lock lock(mutex);
      
        CacheMap::iterator it = cache.find(dirpath);
        if (it != cache.end())
        {
          if (it->second.ts + TIME_OUT < tsNow)
          {
            cache.erase(it);
          }
          else 
          {
            FileMap::iterator fit = it->second.filemap.find(filename);
            if (fit != it->second.filemap.end())
            {
              node = fit->second;
              return 0;
            }
          }
        }
     }

    bool isUnresolvedPath;

    { 
       Lock lock( mutex );
       isUnresolvedPath = lastUnresolvedStatPathTs + TIME_OUT >= tsNow && lastUnresolvedStatPath == dirpath;
    }

    if (isUnresolvedPath) {
      std::vector<SNode> pfilelist;
      int r = sectorClient.list(dirpath, pfilelist,false);
      if (r < 0) return r;
      for (std::vector<SNode>::iterator i = pfilelist.begin(); i != pfilelist.end(); ++ i)
      {
          if (i->m_strName == filename)
          {
              node = *i;
              add(dirpath, pfilelist);
              return 0;
          }
      }
    } else {
      {
        Lock lock (mutex);
        lastUnresolvedStatPath = dirpath;
        lastUnresolvedStatPathTs = tsNow;
      }
      return 1;
    } 

    return 1;

}

