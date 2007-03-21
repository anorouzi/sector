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

#include <server.h>
#include <sql.h>
#include <table.h>

using namespace cb;

void* Server::SQLHandler(void* p)
{
   //Server* self = ((Param3*)p)->s;
   string filename = ((Param3*)p)->fn;
   string query = ((Param3*)p)->q;
   UDTSOCKET u = ((Param3*)p)->u;
   int t = ((Param3*)p)->t;
   int conn = ((Param3*)p)->c;
   delete (Param3*)p;


   SQLExpr sql;
   SQLParser::parse(query, sql);
   Table table;
   table.loadDataFile(filename);
   table.loadSemantics(filename + ".sem");
   EvalTree* tree;
   SQLParser::buildTree(sql.m_Condition, 0, sql.m_Condition.size(), tree);
   bool project;
   if ((sql.m_vstrFieldList.size() == 1) && (sql.m_vstrFieldList[0] == "*"))
      project = false;
   else
      project = true;

   int32_t cmd;
   bool run = true;

   UDTSOCKET lu = u;
   int lt = t;

   if (1 == conn)
   {
      u = UDT::accept(u, NULL, NULL);
      UDT::close(lu);
   }
   else
   {
      t = accept(t, NULL, NULL);
      ::close(lt);
   }

   while (run)
   {
      if (1 == conn)
      {
         if (UDT::recv(u, (char*)&cmd, 4, 0) < 0)
            continue;
      }
      else
      {
         if (::recv(t, (char*)&cmd, 4, 0) <= 0)
            continue;
      }

      switch (cmd)
      {
      case 1: // fetch
         {
            int numOfRows;

            if (1 == conn)
            {
               if (UDT::recv(u, (char*)&numOfRows, 4, 0) < 0)
                  continue;
            }
            else
            {
               if (::recv(t, (char*)&numOfRows, 4, 0) <= 0)
                  continue;
            }

            char* buf = new char[4096 * numOfRows];
            int size = 0;
            int unitsize = 4096;

            for (int i = 0; i < numOfRows; ++ i)
            {
               unitsize = 4096;
               if (table.readTuple(buf + size, unitsize) < 0)
                  break;
               if (table.select(buf + size, tree))
               {
                  if (project)
                  {
                     char temp[4096];
                     unitsize = table.project(buf + size, temp, sql.m_vstrFieldList);
                     memcpy(buf + size, temp, unitsize);
                  }

                  size += unitsize;
               }
            }

            if (1 == conn)
            {
               if (UDT::send(u, (char*)&size, 4, 0) < 0)
               {
                  run = false;
                  break;
               }

               int h;
               if (UDT::send(u, buf, size, 0, &h) < 0)
                  run = false;
            }
            else
            {
               if (::send(u, (char*)&size, 4, 0) < 0)
               {
                  run = false;
                  break;
               }

               int unit = 1460;
               int ts = 0;
               while (size > 0)
               {
                  int ss = ::send(t, buf + ts, (size > unit) ? unit : size, 0);
                  if (ss < 0)
                  {
                     run = false;
                     break;
                  }
                  size -= ss;
                  ts += ss;
               }
            }

            break;
        }

      case 2: // close
         run = false;

      default:
         break;
      }
   }

   return NULL;
}

