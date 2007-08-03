#include <gmp.h>
#include <message.h>
#include <unistd.h>
#include <iostream>

using namespace std;
using namespace cb;

int main()
{
   cb::CGMP gmp;

   gmp.init(7000);


   cout << "TEST " << gmp.rtt("127.0.0.1", 6000) << endl;


   CUserMessage req, res;
   req.m_iDataLength = 2000;
   int32_t id;

   while (true)
   {
      id = 0;
      gmp.sendto("127.0.0.1", 6000, id, &req);

      gmp.recv(id, &res);

      cout << "response: " << id << " " << res.m_pcBuffer << " " << res.m_iDataLength << " " << gmp.rtt("127.0.0.1", 6000) << endl;

      sleep(1);

      cout << endl << endl;
   }

   return 1;
}

