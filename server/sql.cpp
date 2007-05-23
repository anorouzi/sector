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
   Yunhong Gu [gu@lac.uic.edu], last updated 03/24/2007
*****************************************************************************/

#include <server.h>
#include <sql.h>
#include <table.h>

using namespace cb;

void* Server::SQLHandler(void* p)
{
   Server* self = ((Param3*)p)->serv_instance;
   string filename = self->m_strHomeDir + ((Param3*)p)->filename;
   string query = ((Param3*)p)->query;
   Transport* datachn = ((Param3*)p)->datachn;
   string ip = ((Param3*)p)->client_ip;
   int port = ((Param3*)p)->client_data_port;
   delete (Param3*)p;

   cout << "rendezvous connect " << ip << " " << port << endl;

   if (datachn->connect(ip.c_str(), port) < 0)
      return NULL;

   cout << "run sql " << query << endl;

   SQLExpr sql;
   SQLParser::parse(query, sql);
   Table table;
   table.loadDataFile(filename);
   table.loadSemantics(filename + ".sem");
   EvalTree* tree = SQLParser::buildTree(sql.m_Condition, 0, sql.m_Condition.size() - 1);
   bool project;
   if ((sql.m_vstrFieldList.size() == 1) && (sql.m_vstrFieldList[0] == "*"))
      project = false;
   else
      project = true;

   int32_t cmd;
   bool run = true;

   while (run)
   {
      if (datachn->recv((char*)&cmd, 4) < 0)
         continue;

      switch (cmd)
      {
      case 1: // fetch
         {
            int numOfRows;

            if (datachn->recv((char*)&numOfRows, 4) < 0)
               continue;

            char* buf = new char[4096 * numOfRows];
            int size = 0;
            int unitsize = 4096;
            int rows = 0;

            for (int i = 0; i < numOfRows; ++ i)
            {
               unitsize = 4096;
               if (table.readTuple(buf + size, unitsize) < 0)
                  break;

               //cout << "tuple " << i << " " << *(int*)(buf + size) << " " << unitsize << endl;

               if (table.select(buf + size, tree))
               {
                  if (project)
                  {
                     char temp[4096];
                     unitsize = table.project(buf + size, temp, sql.m_vstrFieldList);
                     memcpy(buf + size, temp, unitsize);
                  }

                  size += unitsize;
                  rows ++;
               }
            }

            if (datachn->send((char*)&rows, 4) < 0)
            {
               run = false;
               break;
            }
            if (datachn->send((char*)&size, 4) < 0)
            {
               run = false;
               break;
            }

            cout << "sening out size " << rows << " " << size << endl;
            if (datachn->send(buf, size) < 0)
               run = false;

            break;
        }

      case 2: // close
         run = false;

      default:
         break;
      }
   }

   datachn->close();
   delete datachn;

   return NULL;
}
