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


#include "table.h"
#include "sql.h"
#include <iostream>
using namespace std;
using namespace cb;

int Table::loadDataFile(const string& filename)
{
   m_DataFile.open(filename.c_str());

   if (m_DataFile.bad())
      return -1;

   return 0;
}

int Table::loadSemantics(const string& semfile)
{
   return Semantics::loadSemantics(semfile, m_AttrList);
}

void Table::close()
{
   m_DataFile.close();
}

int Table::readTuple(char* tuple, int& len)
{
   if (m_DataFile.bad() || m_DataFile.eof())
   {
cout << "bad file!!!!\n";
      return -1;
   }

//cout << "attr " << m_AttrList.size() << endl;

   int size = 0;

   for (vector<DataAttr>::iterator i = m_AttrList.begin(); i != m_AttrList.end(); ++ i)
   {
      switch (i->m_Type)
      {
      case INTEGER:
         size += 4;
         break;

      case CHAR:
         size += 1;
         break;

      case FLOAT:
         size += 4;
         break;

      case BOOLEAN:
         size += 1;
         break;

      case STRING:
      {
         int tmp;
         streampos curr = m_DataFile.tellg();
         m_DataFile.seekg(size, ios::cur);
         m_DataFile.read((char*)&tmp, 4);
         m_DataFile.seekg(curr);
         size += 4 + tmp;
         break;
      }

      default:
         return -1;
      }
   }

   if (len < size)
     return -1;

   m_DataFile.read(tuple, size);
   len = size;
   return 0;
}

DataItem Table::readItem(const char* tuple, const string& attr)
{
   DataItem res;

   char* p = (char*)tuple;
   int size = 0;

   for (vector<DataAttr>::iterator i = m_AttrList.begin(); i != m_AttrList.end(); ++ i)
   {
      if (attr == i->m_strName)
      {
         res.m_Type = i->m_Type;

         switch (i->m_Type)
         {
         case INTEGER:
            res.m_iVal = *(int32_t*)(p + size);
            break;

         case CHAR:
            res.m_cVal = *(p + size);
            break;

         case FLOAT:
            res.m_fVal = *(float*)(p + size);
            break;

         case BOOLEAN:
            res.m_bVal = *(bool*)(p + size);
            break;

         case STRING:
            res.m_iVal = *(int32_t*)(p + size);
            for (int i = 0; i < res.m_iVal; ++ i)
               res.m_strVal.append(1, *p++);
            break;

         default:
            return res;
         }

         return res;
      }

      switch (i->m_Type)
      {
      case INTEGER:
         size += 4;
         break;

      case CHAR:
         size += 1;
         break;

      case FLOAT:
         size += 4;
         break;

      case BOOLEAN:
         size += 1;
         break;

      case STRING:
         size += 4 + *(int*)(p + size);
         break;

      default:
         return res;
      }
   }

   return res;
}


bool Table::select(const char* tuple, EvalTree* tree)
{
   if (NULL == tree)
      return true;

   DataItem res;
   if (0 != evaluate(tuple, tree, res))
      return false;

   switch (res.m_Type)
   {
   case INTEGER:
      return 0 != res.m_iVal;
   case CHAR:
      return 0 != res.m_cVal;
   case FLOAT:
      return 0.0 != res.m_fVal;
   case BOOLEAN:
      return res.m_bVal;
   case STRING:
      return 0 != res.m_strVal.length();
   default:
      return false;
   }
}

int Table::project(const char* src, char* dst, const vector<string>& attr)
{
   char* p = dst;
   for (vector<string>::const_iterator i = attr.begin(); i != attr.end(); ++ i)
   {
      DataItem item = readItem(src, *i);
      switch (item.m_Type)
      {
      case INTEGER:
         *(int*)p = item.m_iVal;
         p += 4;
         break;

      case CHAR:
         *p = item.m_cVal;
         p += 1;
         break;

      case FLOAT:
         *(float*)p = item.m_fVal;
         p += 4;
         break;

      case BOOLEAN:
         *(bool*)p = item.m_bVal;
         p += 1;
         break;

      case STRING:
         *(int*)p = item.m_strVal.length();
         strcpy(p + 4, item.m_strVal.c_str());
         p += 4 + item.m_strVal.length();
         break;

      default:
         return -1;
      }
   }

   return p - dst;
}

int Table::evaluate(const char* tuple, EvalTree* tree, DataItem& res)
{
   if (NULL == tree)
      return 0;

   if (CONSTANT == tree->m_Token.m_Type)
   {
      res.m_Type = tree->m_Token.m_DataType;

      switch (tree->m_Token.m_DataType)
      {
      case INTEGER:
         res.m_iVal = atoi(tree->m_Token.m_strToken.c_str());
         break;

      case CHAR:
         res.m_cVal = *(tree->m_Token.m_strToken.c_str());
         break;

      case FLOAT:
         res.m_fVal = atof(tree->m_Token.m_strToken.c_str());
         break;

      case BOOLEAN:
         res.m_bVal = ("FALSE" != tree->m_Token.m_strToken);
         break;

      default:
         return -1;
      }

      return 0;
   }
   else if (UNKNOWN == tree->m_Token.m_Type)
   {
      res = readItem(tuple, tree->m_Token.m_strToken);
      return 0;
   }
   else if (BOOL_OP == tree->m_Token.m_Type)
   {
      res.m_Type = BOOLEAN;

      DataItem r1, r2;
      evaluate(tuple, tree->m_Left, r1);
      evaluate(tuple, tree->m_Right, r2);

      if ("NOT" == tree->m_Token.m_strToken)
      {
         res.m_bVal = !r2.m_bVal;
      }
      else if ("AND" == tree->m_Token.m_strToken)
      {
         res.m_bVal = r1.m_bVal && r2.m_bVal;
      }
      else if ("OR" == tree->m_Token.m_strToken)
      {
         res.m_bVal = r1.m_bVal || r2.m_bVal;
      }
      else if (("=" == tree->m_Token.m_strToken) || ("==" == tree->m_Token.m_strToken))
      {
         if (r1.m_Type != r2.m_Type)
            res.m_bVal = false;

         switch (r1.m_Type)
         {
         case INTEGER:
            res.m_bVal = r1.m_iVal == r2.m_iVal;
            break;

         case CHAR:
            res.m_bVal = r1.m_cVal == r2.m_cVal;
            break;

         case FLOAT:
            res.m_bVal = r1.m_fVal == r2.m_fVal;
            break;

         case BOOLEAN:
            res.m_bVal = r1.m_bVal == r2.m_bVal;
            break;

         case STRING:
            res.m_bVal = r1.m_strVal == r2.m_strVal;
            break;

         default:
            res.m_bVal = false;
         }
      }
      else
      {
         if (">" == tree->m_Token.m_strToken)
            res.m_bVal = (INTEGER == r1.m_Type) ? r1.m_iVal : r1.m_fVal > (INTEGER == r2.m_Type) ? r2.m_iVal : r2.m_fVal;
         else if (">=" == tree->m_Token.m_strToken)
            res.m_bVal = (INTEGER == r1.m_Type) ? r1.m_iVal : r1.m_fVal >= (INTEGER == r2.m_Type) ? r2.m_iVal : r2.m_fVal;
         else if ("<" == tree->m_Token.m_strToken)
            res.m_bVal = (INTEGER == r1.m_Type) ? r1.m_iVal : r1.m_fVal < (INTEGER == r2.m_Type) ? r2.m_iVal : r2.m_fVal;
         else if ("<=" == tree->m_Token.m_strToken)
            res.m_bVal = (INTEGER == r1.m_Type) ? r1.m_iVal : r1.m_fVal <= (INTEGER == r2.m_Type) ? r2.m_iVal : r2.m_fVal;
      }
   }
   else if (ARITH_OP == tree->m_Token.m_Type)
   {
      DataItem r1, r2;
      evaluate(tuple, tree->m_Left, r1);
      evaluate(tuple, tree->m_Right, r2);

      if ((INTEGER == r1.m_Type) && (INTEGER == r2.m_Type))
      {
         res.m_Type = INTEGER;
         if ("+" == tree->m_Token.m_strToken)
            res.m_iVal = r1.m_iVal + r2.m_iVal;
         else if ("-" == tree->m_Token.m_strToken)
            res.m_iVal = r1.m_iVal - r2.m_iVal;
         else if ("*" == tree->m_Token.m_strToken)
            res.m_iVal = r1.m_iVal * r2.m_iVal;
         else if ("/" == tree->m_Token.m_strToken)
            res.m_iVal = r1.m_iVal / r2.m_iVal;
      }
      else
      {
         res.m_Type = FLOAT;
         if ("+" == tree->m_Token.m_strToken)
            res.m_fVal = (INTEGER == r1.m_Type) ? r1.m_iVal : r1.m_fVal + (INTEGER == r2.m_Type) ? r2.m_iVal : r2.m_fVal;
         else if ("-" == tree->m_Token.m_strToken)
            res.m_fVal = (INTEGER == r1.m_Type) ? r1.m_iVal : r1.m_fVal - (INTEGER == r2.m_Type) ? r2.m_iVal : r2.m_fVal;
         else if ("*" == tree->m_Token.m_strToken)
            res.m_fVal = (INTEGER == r1.m_Type) ? r1.m_iVal : r1.m_fVal * (INTEGER == r2.m_Type) ? r2.m_iVal : r2.m_fVal;
         else if ("/" == tree->m_Token.m_strToken)
            res.m_fVal = (INTEGER == r1.m_Type) ? r1.m_iVal : r1.m_fVal / (INTEGER == r2.m_Type) ? r2.m_iVal : r2.m_fVal;
      } 
   }

   return 0;
}
