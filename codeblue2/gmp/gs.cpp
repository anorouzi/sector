#include <gmp.h>
#include <message.h>
#include <iostream>

using namespace std;

int main()
{
   CGMP gmp;

   gmp.init(6000);

   char ip[64];
   int port;

   CUserMessage msg;
   int32_t id;

   char* res = "got it.";
   strcpy(msg.m_pcBuffer, res);
   msg.m_iDataLength = strlen(res) + 1;

   while (true)
   {
      gmp.recvfrom(ip, port, id, &msg);

      cout << "RECV " << ip << " " << port << " " << id << " " << msg.m_pcBuffer << " " << msg.m_iDataLength << endl;

      strcpy(msg.m_pcBuffer, res);
      msg.m_iDataLength = strlen(res) + 1;
      gmp.sendto(ip, port, id, &msg);

      cout << endl << endl;
   }

   return 1;
}
