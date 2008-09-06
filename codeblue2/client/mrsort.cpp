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

   SysStat sys;
   Sector::sysinfo(sys);
   const int fn = sys.m_llTotalSlaves;
   const int32_t N = (int)log2(fn * 50.0f);
   const int rn = (int)pow(2.0f, N);

   vector<string> files;
   for (int i = 0; i < fn; ++ i)
   {
      char filename[256];
      sprintf(filename, "test/sort_input.%d.dat", i);
      files.insert(files.end(), filename);
   }

   SphereStream input;
   if (input.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   SphereStream output;
   output.setOutputPath("test/mr_sorted", "stream_sort_bucket");
   output.init(rn);

   SphereProcess myproc;

   if (myproc.loadOperator("./examples/mr_sort.so") < 0)
      return -1;

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   if (myproc.run_mr(input, output, "mr_sort", 1, (char*)&N, 4) < 0)
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
