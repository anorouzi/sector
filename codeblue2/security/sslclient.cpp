#include <string.h>
#include "ssltransport.h"

int main(int argc, char** argv)
{
   SSLTransport::init();

   SSLTransport client;
   client.initClientCTX("host.cert");
   client.open(NULL, 0);
   client.connect(argv[1], atoi(argv[2]));

   char* msg = "hello!";
   client.send(msg, strlen(msg) + 1);

   SSLTransport::destroy();
   return 1;
}
