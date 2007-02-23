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


#include "data.h"
#include <iostream>

int Semantics::loadSemantics(const string& semfile, vector<DataAttr>& attrlist)
{
   attrlist.clear();

   ifstream ifs(semfile.c_str());
   if (ifs.fail())
      goto ERROR;

   char line[1024];
   line[0] = '\0';

   // skip all comments and blank lines
   while (!ifs.eof())
   {
      line[0] = '\0';
      ifs.getline(line, 1024);
      if (0 == strcmp(line, "BEGIN"))
         break;
   }

   if (0 != strcmp(line, "BEGIN"))
      goto ERROR;

   while (!ifs.eof())
   {
      line[0] = '\0';
      ifs.getline(line, 1024);

      if (0 == strcmp(line, "END"))
         return 0;

      DataAttr attr;
      string token;
      char* p = line;

      if (0 != getToken(&p, token))
         break;
      attr.m_strName = token;

      if (0 != getToken(&p, token))
         break;

      if ("INTEGER" == token)
         attr.m_Type = INTEGER;
      else if ("CHAR" == token)
         attr.m_Type = CHAR;
      else if ("FLOAT" == token)
         attr.m_Type = FLOAT;
      else if ("BOOLEAN" == token)
         attr.m_Type = BOOLEAN;
      else if ("STRING" == token)
         attr.m_Type = STRING;
      else
         goto ERROR;

      attrlist.insert(attrlist.end(), attr);
   }

ERROR:
   ifs.close();
   return -1;
}

int Semantics::getToken(char** line, string& token)
{
   char* p = *line;

   while (((' ' == *p) || ('\t' == *p)) && ('\0' != *p))
      ++ p;

   if ('\0' == *p)
      return -1;

   token = "";
   while ((' ' != *p) && ('\t' != *p) && ('\0' != *p))
      token.append(1, *p++);

   *line = p;

   return 0;
}

int Semantics::serialize(string& semstring, vector<DataAttr>& attrlist)
{
   semstring = "";

   for (vector<DataAttr>::iterator i = attrlist.begin(); i != attrlist.end(); ++ i)
   {
      char line[1024];
      sprintf(line, "%s %d ", i->m_strName.c_str(), i->m_Type);
      semstring += line;
   }

   return semstring.length();
}

int Semantics::deserialize(const string& semstring, vector<DataAttr>& attrlist)
{
   char* p = (char*)semstring.c_str();
   string token;

   attrlist.clear();

   for (;;)
   {
      DataAttr attr;

      if (0 != getToken(&p, token))
         break;
      attr.m_strName = token;

      if (0 != getToken(&p, token))
         return -1;
      attr.m_Type = (DataType)atoi(token.c_str());

      attrlist.insert(attrlist.end(), attr); 
   }

   return attrlist.size();
}

void Semantics::display(vector<DataAttr>& attrlist)
{
   if (0 == attrlist.size())
   {
      cout << "NULL\n";
      return;
   }

   for (vector<DataAttr>::iterator i = attrlist.begin(); i != attrlist.end(); ++ i)
   {
      cout << i->m_strName << " " << i->m_Type << endl;
   }
}
