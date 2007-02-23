#ifndef __INDEX_H__
#define __INDEX_H__

#include <util.h>
#include <file.h>
#include <map>
#include <set>
#include <vector>
#include <iostream>

using namespace std;

class CIndex
{
public:
   CIndex();
   ~CIndex();

public:
   int lookup(const string& filename, set<CFileAttr, CAttrComp>* attr = NULL);
   int insert(const CFileAttr& attr);
   int remove(const string& filename);

public:
   int getFileList(map<string, set<CFileAttr, CAttrComp> >& list);

private:
   map<string, set<CFileAttr, CAttrComp> > m_mFileList;

   pthread_mutex_t m_IndexLock;
};

struct CIndexInfo
{
   char m_pcName[64];           // unique file name
   int64_t m_llTimeStamp;         // time stamp
   //char m_pcType[64];           // file type, data, video, audio, etc
   int64_t m_llSize;            // size

   timeval m_LRT;		// last time the file information is reported (Last Report Time)
};

class CNameIndex
{
public:
   CNameIndex();
   ~CNameIndex();

public:
   int insert(const CIndexInfo& file);
   int remove(const CIndexInfo& file);

public:
   int serialize(char* buffer, int& len);
   int deserialize(const char* buffer, const int& len);

private:
   map<string, CIndexInfo> m_mFileList;
};

#endif
