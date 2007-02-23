#include <index.h>
#include <util.h>

using namespace std;
using namespace CodeBlue;

CIndex::CIndex()
{
   Sync::initMutex(m_IndexLock);
}

CIndex::~CIndex()
{
   Sync::releaseMutex(m_IndexLock);
}

int CIndex::lookup(const string& filename, set<CFileAttr, CAttrComp>* filelist)
{
   int res = -1;

   Sync::enterCS(m_IndexLock);

   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(filename);

   if (i !=  m_mFileList.end())
   {
      if (NULL != filelist)
         *filelist = i->second;
      res =  1;
   }

   Sync::leaveCS(m_IndexLock);

   return res;
}

int CIndex::insert(const CFileAttr& attr)
{
   Sync::enterCS(m_IndexLock);

   map<string, set<CFileAttr, CAttrComp> >::iterator i = m_mFileList.find(attr.m_pcName);

   if (i == m_mFileList.end())
   {
      set<CFileAttr, CAttrComp> sa;
      m_mFileList[attr.m_pcName] = sa;
   }

   m_mFileList[attr.m_pcName].insert(attr);

   Sync::leaveCS(m_IndexLock);

   return 1;
}
   
int CIndex::remove(const string& filename)
{
   Sync::enterCS(m_IndexLock);
   m_mFileList.erase(filename);
   Sync::leaveCS(m_IndexLock);

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

int CNameIndex::insert(const CIndexInfo& file)
{
   m_mFileList[file.m_pcName] = file;
   return 1;
}

int CNameIndex::remove(const CIndexInfo& file)
{
   map<string, CIndexInfo>::iterator i = m_mFileList.find(file.m_pcName);

   if (i == m_mFileList.end())
      return -1;

   m_mFileList.erase(i);

   return 1;
}

int CNameIndex::serialize(char* buffer, int& len)
{
   if (len < m_mFileList.size() * sizeof(CIndexInfo))
     return -1;

   len = m_mFileList.size() * sizeof(CIndexInfo);

   int c = 0;

   for (map<string, CIndexInfo>::iterator i = m_mFileList.begin(); i != m_mFileList.end(); ++ i)
   {
      memcpy(buffer + c * sizeof(CIndexInfo), (char*)&(i->second), sizeof(CIndexInfo));
      ++ c;
   }

   return len;
}

int CNameIndex::deserialize(const char* buffer, const int& len)
{
   m_mFileList.clear();

   for (unsigned int i = 0; i < len/sizeof(CIndexInfo); ++ i)
      insert(((CIndexInfo*)buffer)[i]);

   return len/sizeof(CIndexInfo);
}
