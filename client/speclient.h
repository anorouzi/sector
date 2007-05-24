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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/22/2007
*****************************************************************************/

#ifndef __SECTOR_H__
#define __SECTOR_H__

#include "client.h"
#include <string>
#include <pthread.h>
#include <transport.h>

using namespace std;

namespace cb
{

class Process
{
friend class Client;

public:
   Process();
   ~Process();

   int open(vector<string> stream, string op, const char* param = NULL, const int& size = 0);
   int close();

   int run();
   int checkProgress();
   int read(char*& data, int& size, string& file, int64_t& offset, int& rows, const bool& inorder = true, const bool& wait = true);

private:
   string m_strOperator;
   string m_strParam;
   vector<string> m_vstrStream;

   struct DS
   {
      string m_strDataFile;
      int64_t m_llOffset;
      int64_t m_llSize;

      int m_iSPEID;

      int m_iStatus;		// 0: not started yet; 1: in progress; 2: done, result ready; 3: result read
      char* m_pcResult;
      int m_iResSize;
   };
   vector<DS> m_vDS;

   struct SPE
   {
      uint32_t m_uiID;
      string m_strIP;
      int m_iPort;

      DS* m_pDS;
      int m_iStatus;		// -1: bad; 0: ready; 1; running
      int m_iProgress;

      timeval m_StartTime;
      timeval m_LastUpdateTime;

      Transport m_DataChn;
   };
   vector<SPE> m_vSPE;

   int m_iProgress;
   int m_iAvgRunTime;
   int m_iTotalDS;
   int m_iTotalSPE;
   int m_iAvailRes;

   pthread_mutex_t m_ResLock;
   pthread_cond_t m_ResCond;

   int m_iMinUnitSize;
   int m_iMaxUnitSize;

   CGMP m_GMP;

private:
   static void* run(void*);

   int checkSPE();
   int startSPE(SPE& s);
};

}; // namespace cb

#endif
