#include <index.h>

using namespace std;

CIndex::CIndex()
{
   pthread_mutex_init(&m_IndexLock, NULL);
}

CIndex::~CIndex()
{
   pthread_mutex_destroy(&m_IndexLock);
}

int CIndex::lookup(const string& filename, set<CFileAttr, CAttrComp>* filelist)
{
   int res = -1;

   pthread_mutex_lock(&m_IndexLock);

   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(filename);

   if (i !=  m_mFileList.end())
   {
      if (NULL != filelist)
         *filelist = i->second;
      res =  1;
   }

   pthread_mutex_unlock(&m_IndexLock);

   return res;
}

int CIndex::insert(const CFileAttr& attr)
{
   pthread_mutex_lock(&m_IndexLock);

   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(attr.m_pcName);

   if (i == m_mFileList.end())
   {
      set<CFileAttr, CAttrComp> sa;
      m_mFileList[attr.m_pcName] = sa;
   }

   m_mFileList[attr.m_pcName].insert(attr);

   pthread_mutex_unlock(&m_IndexLock);

   return 1;
}
   
int CIndex::remove(const string& filename)
{
   pthread_mutex_lock(&m_IndexLock);

   m_mFileList.erase(filename);

   pthread_mutex_unlock(&m_IndexLock);

   return 1;
}

int CIndex::getFileList(map<string, set<CFileAttr, CAttrComp> >& list)
{
   list.clear();

   list = m_mFileList;

   return list.size();
}


CNameIndex::CNameIndex()
{
   m_mFileList.clear();
}

CNameIndex::~CNameIndex()
{
}

int CNameIndex::search(vector<string>& files)
{
   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.begin(); i != m_mFileList.end(); ++ i)
      files.insert(files.end(), i->first);

   return files.size();
}

int CNameIndex::insert(const string& filename, const string& host, const int& port)
{
   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(filename);

   set<CFileAttr, CAttrComp> sa;

   if (i != m_mFileList.end())
      m_mFileList[filename] = sa;

   CFileAttr attr;
   strcpy(attr.m_pcName, filename.c_str());
   strcpy(attr.m_pcHost, host.c_str());
   attr.m_iPort = port;

   m_mFileList[filename].insert(attr);

   return 1;
}

int CNameIndex::remove(const string& filename, const string& host, const int& port)
{
   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(filename);

   if (i == m_mFileList.end())
      return 1;

   CFileAttr attr;
   strcpy(attr.m_pcName, filename.c_str());
   strcpy(attr.m_pcHost, host.c_str());
   attr.m_iPort = port;

   i->second.erase(attr);

   return 1;
}

void CNameIndex::synchronize(vector<string>& files, char* buffer, int& len)
{
   len = files.size() * 64;

   int c = 0;

   for (vector<string>::iterator i = files.begin(); i != files.end(); ++ i)
   {
      strcpy(buffer + c * 64, i->c_str());
      ++ c;
   }
}

void CNameIndex::desynchronize(vector<string>& files, const char* buffer, const int& len)
{
   for (int i = 0; i < len/64; ++ i)
      files.insert(files.end(), buffer + i * 64);
}
