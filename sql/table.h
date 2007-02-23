/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


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
   void close();

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
