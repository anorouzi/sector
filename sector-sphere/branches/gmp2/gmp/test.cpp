/*****************************************************************************
Copyright (c) 2011, VeryCloud LLC.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/*****************************************************************************
written by
   Brian Griffin, last updated 05/11/2011
*****************************************************************************/

#include "gmp.h"
#include <iostream>

using namespace std;

#ifndef WIN32
void* Test_1_Srv(void* param)
#else
DWORD WINAPI Test_1_Srv(LPVOID param)
#endif
{
   CGMP gmp;
   gmp.init(2200);

   string ip;
   int port;
   CUserMessage msg;
   int32_t id;
   const char* res = "got it.";

   strcpy(msg.m_pcBuffer, res);
   msg.m_iDataLength = strlen(res) + 1;

   // Receive UDP messages.
   for (int i = 0; i < 10; ++ i)
   {
      gmp.recvfrom(ip, port, id, &msg);

      strcpy(msg.m_pcBuffer, res);
      msg.m_iDataLength = strlen(res) + 1;
      gmp.sendto(ip, port, id, &msg);
   }

   // Receive UDT messages.
   for (int i = 0; i < 10; ++ i)
   {
      gmp.recvfrom(ip, port, id, &msg);

      strcpy(msg.m_pcBuffer, res);
      msg.m_iDataLength = strlen(res) + 1;
      gmp.sendto(ip, port, id, &msg);
   }

   gmp.close();

   return 0;
}

#ifndef WIN32
void* Test_1_Cli(void* param)
#else
DWORD WINAPI Test_1_Cli(LPVOID param)
#endif
{
   CGMP gmp;
   gmp.init(2210);

   cout << "RTT= " << gmp.rtt("127.0.0.1", 2200) << endl;

   // Test small messages for UDP.
   CUserMessage req, res;
   req.m_iDataLength = 1000;
   int32_t id;

   for (int i = 0; i < 10; ++ i)
   {
      id = 0;
      gmp.sendto("127.0.0.1", 2200, id, &req);
      gmp.recv(id, &res);
      cout << "response: " << id << " " << res.m_pcBuffer << " " << res.m_iDataLength << " " << gmp.rtt("127.0.0.1", 6000) << endl;
   }

   // Test large messages for UDT.
   req.m_iDataLength = 2000;
   for (int i = 0; i < 10; ++ i)
   {
      id = 0;
      gmp.sendto("127.0.0.1", 2200, id, &req);
      gmp.recv(id, &res);
   }

   gmp.close();

   return 0;
}

int main()
{
   const int test_case = 1;

#ifndef WIN32
   void* (*Test_Srv[test_case])(void*);
   void* (*Test_Cli[test_case])(void*);
#else
   DWORD (WINAPI *Test_Srv[test_case])(LPVOID);
   DWORD (WINAPI *Test_Cli[test_case])(LPVOID);
#endif

   Test_Srv[0] = Test_1_Srv;
   Test_Cli[0] = Test_1_Cli;

   for (int i = 0; i < test_case; ++ i)
   {
#ifndef WIN32
      pthread_t srv, cli;
      pthread_create(&srv, NULL, Test_Srv[i], NULL);
      pthread_create(&cli, NULL, Test_Cli[i], NULL);

      pthread_join(srv, NULL);
      pthread_join(cli, NULL);
#else
      HANDLE srv, cli;
      srv = CreateThread(NULL, 0, Test_Srv[i], NULL, 0, NULL);
      cli = CreateThread(NULL, 0, Test_Cli[i], NULL, 0, NULL);

      WaitForSingleObject(srv, INFINITE);
      WaitForSingleObject(cli, INFINITE);
#endif
   }

   return 0;
}
