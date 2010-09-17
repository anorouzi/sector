/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/

#include <iostream>
#include <sector.h>
#include <conf.h>

using namespace std;

void print_error(int code)
{
   cerr << "ERROR: " << code << " " << SectorError::getErrorMsg(code) << endl;
}

int main(int argc, char** argv)
{
   if (argc != 3)
   {
      cerr << "USAGE: mv <src_file/dir> <dst_file/dir>\n";
      return -1;
   }

   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   int result = 0;
   if ((result = client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort)) < 0)
   {
      print_error(result);
      return -1;
   }
   if ((result = client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str())) < 0)
   {
      print_error(result);
      return -1;
   }

   string path = argv[1];
   bool wc = WildCard::isWildCard(path);

   if (!wc)
   {
      if ((result = client.move(argv[1], argv[2])) < 0)
         print_error(result);
   }
   else
   {
      SNode attr;
      if (client.stat(argv[2], attr) < 0)
      {
         cerr << "destination directory does not exist.\n";
      }
      else
      {
         string orig = path;
         size_t p = path.rfind('/');
         if (p == string::npos)
            path = "/";
         else
         {
            path = path.substr(0, p);
            orig = orig.substr(p + 1, orig.length() - p);
         }

         vector<SNode> filelist;
         if ((result = client.list(path, filelist)) < 0)
            print_error(result);

         vector<string> filtered;
         for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
         {
            if (WildCard::match(orig, i->m_strName))
               filtered.push_back(path + "/" + i->m_strName);
         }

         for (vector<string>::iterator i = filtered.begin(); i != filtered.end(); ++ i)
            client.move(*i, argv[2]);
      }
   }

   client.logout();
   client.close();

   return 0;
}
