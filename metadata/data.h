#ifndef __CB_DATA_H__
#define __CB_DATA_H__


#include <fstream>
#include <vector>
#include <string>

using namespace std;

enum DataType {INTEGER, CHAR, FLOAT, BOOLEAN, STRING};
enum BOOLEAN {TRUE, FALSE};

struct DataItem
{
   DataType m_Type;
   union
   {
      int m_iVal;
      char m_cVal;
      float m_fVal;
      bool m_bVal;
   };
   string m_strVal;
};

struct DataAttr
{
   string m_strName;
   DataType m_Type;
};

class Semantics
{
public:
   static int loadSemantics(const string& semfile, vector<DataAttr>& attrlist);

   static int serialize(string& semstring, vector<DataAttr>& attrlist);
   static int deserialize(const string& semstring, vector<DataAttr>& attrlist);

   static void display(vector<DataAttr>& attrlist);

private:
   static int getToken(char** line, string& token);
};


#endif
