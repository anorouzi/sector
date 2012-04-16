#include <iostream>

#include "fusedircache.h"
#include "sectorfs.h"

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
    ClientConf& config = SectorFS::g_SectorConfig.m_ClientConf;
    
    pthread_mutex_lock( &mutex );

    time_t tsNow = time(0);

    for( CacheMap::iterator nit = cache.begin(), end = cache.end(); nit != end; )
    {
       if( nit->second.expirationTime < tsNow )
           cache.erase( nit++ );
       else
           ++nit;
    }

    pthread_mutex_unlock( &mutex );

    time_t expirationDuration = DEFAULT_TIME_OUT;

    typedef ClientConf::CacheLifetimes CacheLifetimes;
    for( CacheLifetimes::const_iterator i = config.m_pathCache.begin(), end = config.m_pathCache.end(); i != end; ++i )
        if( WildCard::match( i->m_sPathMask, path ) )
        {
            expirationDuration = i->m_seconds;
            //std::cout << "Add: path " << path << " found cache lifetime " << i->m_sPathMask << " " << i->m_seconds << std::endl;
            break;
        }

    pthread_mutex_lock( &mutex );

    // add new entry with specified lifetime

    CacheRec& rec = cache[path];
    rec.path = path;
    rec.expirationTime = time(0) + expirationDuration;
// clear map if record already present
    rec.filemap.clear();
    
    std::vector<SNode>::const_iterator fit = filelist.begin(), fend = filelist.end();
    while (fit != fend)
    {
      rec.filemap[fit->m_strName] = *fit;
      ++fit;
    }

    pthread_mutex_unlock( &mutex );

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
          if (it->second.expirationTime < tsNow)
          {
            cache.erase(it);
          }
          else 
          {
            //std::cout << "Get: path " << path << " hit cache" << std::endl;
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
       isUnresolvedPath = lastUnresolvedStatPathTs + DEFAULT_TIME_OUT >= tsNow && lastUnresolvedStatPath == dirpath;
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


int DirCache::readdir( std::string dirpath, std::vector<SNode>& filelist) {
// return codes : 1 - cache miss, 0 - cache hit
    time_t tsNow = time(0);

    if( dirpath.empty() )
        dirpath = '/';
    else if( dirpath[0] != '/' )
        dirpath = '/' + dirpath;

    if( dirpath.length() > 1 && dirpath[ dirpath.length() - 1 ] == '/' )
        dirpath.erase( dirpath.length() - 1 );

    Lock lock(mutex);
      
    CacheMap::iterator it = cache.find(dirpath);
    if (it != cache.end())
    {
      if (it->second.expirationTime < tsNow)
      {
        cache.erase(it);
        return 1;
      }
      else 
      {
        //std::cout << "Readdir: path " << dirpath << " hit cache" << std::endl;
        filelist.clear();
        filelist.reserve( it->second.filemap.size() );
        for( FileMap::iterator fit = it->second.filemap.begin(), end = it->second.filemap.end(); fit != end; ++fit )
            filelist.push_back( fit->second );
        return 0;
      }
    }

    return 1;
}
