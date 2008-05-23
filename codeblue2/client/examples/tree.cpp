#include "fsclient.h"
#include "dcclient.h"
#include <iostream>
#include <cmath>

using namespace std;
using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

   vector<string> files;
   files.insert(files.end(), "rand1.dat");
   files.insert(files.end(), "rand2.dat");
   files.insert(files.end(), "rand3.dat");
   files.insert(files.end(), "rand4.dat");
   //files.insert(files.end(), "rand5.dat");
   files.insert(files.end(), "rand6.dat");
   //files.insert(files.end(), "rand7.dat");
   files.insert(files.end(), "rand8.dat");
   files.insert(files.end(), "rand9.dat");
   files.insert(files.end(), "rand10.dat");

   Stream s;
   if (s.init(files) < 0)
   {
      cout << "unable to locate input data files. quit.\n";
      return -1;
   }

   Stream buckets;
   buckets.m_strName = "stream_sort_bucket";
   buckets.init(256);

   Process* myproc = Sector::createJob();

   timeval t;
   gettimeofday(&t, 0);
   cout << "start time " << t.tv_sec << endl;

   int n = 8;
   if (myproc->run(s, buckets, "sorthash", 1, (char*)&n, sizeof(int)) < 0)
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

   if (myproc->run(buckets, output, "sort", 0, NULL, 0, 1) < 0)
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


   cout << "start building tree...\n";

   int* index = new int[buckets.m_llRecNum];
   int gi = 0;
   File* f = Sector::createFileHandle();
   for (int i = 0; i < buckets.m_iFileNum; ++ i)
   {
      if (buckets.m_vSize[i] <= 0)
         continue;

      cout << "READING FILE " << buckets.m_vLocation[i].begin()->m_pcIP << " " << buckets.m_vLocation[i].begin()->m_iAppPort << " " << buckets.m_vFiles[i] << endl;

      char loc[68];
      memcpy(loc, buckets.m_vLocation[i].begin()->m_pcIP, 64);
      *(int*)(loc + 64) = buckets.m_vLocation[i].begin()->m_iAppPort;
      f->open(buckets.m_vFiles[i], 1, NULL, loc, 1);

      char* tmp = new char[buckets.m_vSize[i]];
      f->read(tmp, 0, buckets.m_vSize[i]);

      cout << "data " << buckets.m_llRecNum << " " << buckets.m_vRecNum[i] << endl;

      for (int j = 0; j < buckets.m_vRecNum[i]; ++ j)
         index[gi ++] = *(int*)(tmp + 100 * j) % 2;

      delete [] tmp;

      f->close();
   }
   Sector::releaseFileHandle(f);

   for (int i = 1; i < buckets.m_llRecNum; ++ i)
      index[i] = index[i - 1] + index[i];

   int N = buckets.m_llRecNum;	// total numbe rof records
   int M = N - index[N - 1]; 	// total number of zeros
   double delta = 0;
   int di = 1;

   for (int i = 1; i < N - 1; ++ i)
   {
      double pa0 = double(i + 1 - index[i]) / (i + 1);
      double pa1 = double(index[i]) / (i + 1);
      double ha = - pa0 * log(pa0) - pa1 * log(pa1);

      double qa0 = double(M - i - 1 + index[i]) / (N - i - 1);
      double qa1 = double(N - M - index[i]) / (N - i - 1);
      double hb = - qa0 * log(qa0) - qa1 * log(qa1);

      double d = ((i + 1) * ha + (N - i - 1) * hb) / N;

      if (d > delta)
      {
         delta = d;
         di = i;
      }
   }

   cout << "mission accomplished " << di << " " << delta << endl;


   Sector::close();
   
   return 0;
}


// user defined process

//int myProc(const char* unit, const int& rows, const int64_t* index, char* result, int& rsize, int& rrows, int64_t* rindex, int& bid, const char* param, const int& psize)
//{
//}
