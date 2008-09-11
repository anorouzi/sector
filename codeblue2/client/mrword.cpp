#include "dcclient.h"
#include <iostream>
#include <cmath>
using namespace std;

int main(int argc, char** argv)
{
   if (3 != argc)
   {
      cout << "usage: mrsort <ip> <port>" << endl;
      return 0;
   }

   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   vector<string> files;
   files.insert(files.end(), "/html");

   SphereStream input;
   if (input.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream output;
   output.setOutputPath("/mrword", "inverted_index");
   output.init(256);

   SphereProcess myproc;

   if (myproc.loadOperator("./examples/mr_word.so") < 0)
      return -1;

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   if (myproc.run_mr(input, output, "mr_word", 0) < 0)
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
         if (myproc.checkMapProgress() == -1)
         {
            cerr << "all SPEs failed\n";
            break;
         }

         if (myproc.checkMapProgress() == 100)
            break;
      }

      gettimeofday(&t2, 0);
      if (t2.tv_sec - t1.tv_sec > 60)
      {
         cout << "MAP PROGRESS: " << myproc.checkProgress() << "%" << endl;
         t1 = t2;
      }
   }

   while (myproc.checkReduceProgress() < 100)
   {
      usleep(10);
   }

   gettimeofday(&t, 0);
   cout << "mapreduce sort accomplished " << t.tv_sec << endl;

   cout << "SPE COMPLETED " << endl;

   myproc.close();

   Sector::logout();
   Sector::close();

   return 0;
}
