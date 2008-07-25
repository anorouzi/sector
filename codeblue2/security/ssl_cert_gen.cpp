#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>

using namespace std;

void gen_cert(const string& name)
{
   string keyname = name + "_node.key";
   string certname = name + "_node.cert";

   system(("openssl genrsa 1024 > " + keyname).c_str());
   system(("chmod 400 " + keyname).c_str());
   system(("openssl req -new -x509 -nodes -sha1 -days 365 -batch -key " + keyname + " > " + certname).c_str());
}

int main(int argc, char** argv)
{
   if (argc != 2)
   {
      cout << "usage: ssl_cert_gen security OR ssl_cert_gen_master" << endl;
      return -1;
   }

   if (0 == strcmp(argv[1], "security"))
   {
      struct stat s;
      if (stat("security_node.key", &s) != -1)
      {
         cerr << "Key already exist\n";
         return -1;
      }

      gen_cert("security");
      system("cp security_node.cert ../master");
   }
   else if (0 == strcmp(argv[1], "master"))
   {
      struct stat s;
      if (stat("master_node.key", &s) != -1)
      {
         cerr << "Key already exist\n";
         return -1;
      }

      gen_cert("master");
      system("cp master_node.cert ../client");
      system("cp master_node.cert ../slave");
   }
   else
      return -1;

   return 0;
}
