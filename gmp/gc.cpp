#include <gmp.h>
#include <unistd.h>
#include <iostream>

using namespace std;

int main()
{
   CGMP gmp;

   gmp.init(7000);


   cout << "TEST " << gmp.rtt("127.0.0.1", 6000) << endl;

//   char ip[64];
//   int port;
   char* data = "hello world! hello, hello!";
//   int len = 1024;
   int32_t id;
   char res[1024];
   int reslen;

   while (true)
   {
      id = 0;
      gmp.sendto("127.0.0.1", 6000, id, data, strlen(data) + 1);

      gmp.recv(id, res, reslen);

      cout << "response: " << id << " " << res << " " << reslen << " " << gmp.rtt("127.0.0.1", 6000) << endl;

      sleep(1);

      cout << endl << endl;
   }

   return 1;
}

