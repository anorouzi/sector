#include "dcclient.h"
#include <iostream>
#include <cmath>
using namespace std;

int main(int argc, char** argv)
{
   if (argc != 4)
   {
      cerr << "usage: text.idx master_ip master_port text_file_name" << endl;
      return -1;
   }

   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   vector<string> files;
   files.insert(files.end(), argv[3]);

   SphereStream input;
   if (input.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream output;
   output.init(0);

   SphereProcess myproc;

   if (myproc.loadOperator("./examples/gen_idx.so") < 0)
      return -1;

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   if (myproc.run(input, output, "gen_idx", 0) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }

   timeval t1, t2;
   gettimeofday(&t1, 0);
   t2 = t1;
   while (true)
   {
      SphereResult* res;

      if (-1 == myproc.read(res))
      {
         if (myproc.checkProgress() == -1)
         {
            cerr << "all SPEs failed\n";
            break;
         }

         if (myproc.checkProgress() == 100)
            break;
      }

      gettimeofday(&t2, 0);
      if (t2.tv_sec - t1.tv_sec > 60)
      {
         cout << "PROGRESS: " << myproc.checkProgress() << "%" << endl;
         t1 = t2;
      }
   }

   gettimeofday(&t, 0);
   cout << "mission accomplished " << t.tv_sec << endl;

   myproc.close();

   Sector::logout();
   Sector::close();

   return 0;
}
