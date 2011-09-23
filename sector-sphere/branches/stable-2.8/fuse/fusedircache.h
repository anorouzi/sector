/********************************************************************
Short-term non-persistent directory cache for Fuse
Goal of cache is to get rid of as many as possible stat calls, leaving all calls to ls in place
Written by  SergeyC last modified 9/23/11
*********************************************************************/

#ifndef __SECTOR_FUSE_DIR_CACHE_H__
#define __SECTOR_FUSE_DIR_CACHE_H__

#include <list>
#include <fstream>
#include <vector>
#include <pthread.h>

#include "sector.h"

class Lock {
    pthread_mutex_t* mutex;

public:
    Lock(pthread_mutex_t& mux) : mutex(&mux) { pthread_mutex_lock(mutex); }
    ~Lock() { pthread_mutex_unlock(mutex); }
};

class DirCache {
    static const time_t TIME_OUT = 2; // expiration time for cache - 2 sec

    struct CacheRec {
        std::string path;
        time_t ts;
        std::vector<SNode> filelist;
    }; 
    typedef std::list<CacheRec> CacheList;

public:
    DirCache();
    ~DirCache();

    void clear_old();

    void add(const std::string& path, const std::vector<SNode>& filelist);

    int get(const std::string& path, Sector& sectorClient, SNode& node);

    void clear();

    static DirCache& instance()
    {
        if( !inst )
            inst = new DirCache();

        return *inst;
    }

    static void destroy()
    {
        if( inst )
            delete inst;
        inst = NULL;
    }

private:
    SNode rootNode;
    CacheList cache;

    std::string lastUnresolvedStatPath;
    time_t lastUnresolvedStatPathTs;

    pthread_mutex_t mutex;

    std::ofstream log;

    static DirCache* inst;
};

#endif

