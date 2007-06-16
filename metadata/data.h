/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#ifndef __CB_DATA_H__
#define __CB_DATA_H__


#include <fstream>
#include <vector>
#include <string>

using namespace std;

namespace cb
{

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

}; // namespace

#endif
