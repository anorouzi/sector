#ifndef __CB_TABLE_H__
#define __CB_TABLE_H__

#include <data.h>
#include <fstream>
#include <sql.h>

using namespace std;

class Table
{
public:
   int loadDataFile(const string& filename);
   int loadSemantics(const string& semfile);
   int close();

   int readTuple(char* tuple, int& len);
   DataItem readItem(const char* tuple, const string& attr);

   bool select(const char* tuple, EvalTree* tree);
   int project(const char* src, char* dst, const vector<string>& attr);

private:
   int evaluate(const char* tuple, EvalTree* tree, DataItem& res);

private:
   ifstream m_DataFile;
   vector<DataAttr> m_AttrList;
};

#endif
