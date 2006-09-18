#include <gmp.h>
#include <iostream>

using namespace std;

int main()
{
   CGMP gmp;

   gmp.init(6000);

   char ip[64];
   int port;
   char data[1024];
   int len = 1024;
   int32_t id;

   char* res = "got it.";

   while (true)
   {
      gmp.recvfrom(ip, port, id, data, len);

      cout << "RECV " << ip << " " << port << " " << id << " " << data << " " << len << endl;

      gmp.sendto(ip, port, id, res, strlen(res) + 1);

      cout << endl << endl;
   }

   return 1;
}
