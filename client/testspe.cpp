#include "speclient.h"

using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

   vector<string> files;
   files.insert(files.begin(), "stream.dat");

   Stream s;
   s.init(files);

   Stream output;
   output.m_strName = "stream_sort_bucket";
   output.init(4);

   Process* myproc = Sector::createJob();

   if (myproc->run(s, output, "sorthash", 1) < 0)
   {
      cout << "failed to find any computing resources." << endl;
      return -1;
   }

   while (true)
   {
      Result* res;

      if (-1 == myproc->read(res, true))
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

//      cout << "read one block " << res->m_iDataLen << endl;

//      for (int i = 0; i < res->m_iDataLen; i += 4)
//         cout << *(int*)(res->m_pcData + i) << endl;
   }

   cout << "SPE COMPLETED " << output.m_iFileNum << " " << output.m_vFiles[0] << endl;

   myproc->close();
   Sector::releaseJob(myproc);

   Sector::close();

   return 0;
}


// user defined process

//int myProc(const char* unit, const int& rows, const int64_t& index, char* result, int& rsize, int& rrows, int64_t* rindex, int& bid, const char* param, const int& psize)
//{
//}
