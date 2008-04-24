#include "dcclient.h"
#include <iostream>

using namespace std;
using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

   vector<string> files;
   files.insert(files.end(), "rand1.dat");
   //files.insert(files.end(), "rand2.dat");

   Stream s;
   if (s.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   Stream temp;
   temp.m_strName = "stream_sort_bucket";
   temp.init(4);

   Process* myproc = Sector::createJob();

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   int n = 2;
   if (myproc->run(s, temp, "sorthash", 1, (char*)&n, sizeof(int)) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }

   while (true)
   {
      Result* res;

      if (-1 == myproc->read(res))
      {
         if (myproc->checkProgress() == -1)
         {
            cerr << "all SPEs failed\n";
            break;
         }

         if (myproc->checkProgress() == 100)
            break;
         continue;
      }
   }

   gettimeofday(&t, 0);
   cout << "stage 1 accomplished " << t.tv_sec << endl;

   Stream output;
   output.init(0);

   if (myproc->run(temp, output, "sort", 0, NULL, 0, 2) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }

   while (true)
   {
      Result* res;

      if (-1 == myproc->read(res))
      {
         if (myproc->checkProgress() == -1)
         {
            cerr << "all SPEs failed\n";
            break;
         }

         if (myproc->checkProgress() == 100)
            break;
         continue;
      }
   }

   gettimeofday(&t, 0);
   cout << "stage 2 accomplished " << t.tv_sec << endl;

   cout << "SPE COMPLETED " << endl;

   myproc->close();
   Sector::releaseJob(myproc);

   Sector::close();

   return 0;
}


// user defined process

//int myProc(const char* unit, const int& rows, const int64_t* index, char* result, int& rsize, int& rrows, int64_t* rindex, int& bid, const char* param, const int& psize)
//{
//}
