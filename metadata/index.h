#ifndef __INDEX_H__
#define __INDEX_H__

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

class CNameIndex
{
public:
   CNameIndex();
   ~CNameIndex();

public:
   int search(vector<string>& files);

   int insert(const string& filename, const string& host, const int& port);
   int remove(const string& filename, const string& host, const int& port);

public:
   static void synchronize(vector<string>& files, char* buffer, int& len);
   static void desynchronize(vector<string>& files, const char* buffer, const int& len);

private:
   map<string, set<CFileAttr, CAttrComp> > m_mFileList;
};

#endif
