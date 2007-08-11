#ifndef __SECTOR_FS_H__
#define __SECTOR_FS_H__

#include <stdint.h>
#include <string>
#include <vector>

using namespace std;

class SectorFS
{
public:
   SectorFS();
   ~SectorFS();

public:
   int init(const string dir);

   int locate(const string& filename, const uint32_t& key, string& loc);
   int create(const string& filename, const uint32_t& key, string& loc);

   int scan(vector<string>& filelist, vector<string>& dirs, const string& currdir);

private:
   static const int m_iLevel = 3;
   string m_strHomeDir;
};

#endif
