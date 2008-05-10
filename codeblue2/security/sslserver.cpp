#include "ssltransport.h"

#include <iostream>
using namespace std;

int main()
{
   SSLTransport::init();

   SSLTransport serv;

   serv.initServerCTX("host.cert", "host.key");
   serv.open("127.0.0.1", 9000);
   serv.listen();

   char ip[64];
   int port;
   SSLTransport* s = serv.accept(ip, port);

   char msg[64];
   s->recv(msg, 64);

   cout << "GOT MSG " << msg << endl;

   s->close();
   serv.close();

   SSLTransport::destroy();
   return 1;
}
