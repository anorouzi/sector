#include "dcclient.h"

using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

   vector<string> files;
   files.insert(files.begin(), "stream.dat");

   Stream s;
   s.init(files);

   Stream temp;
   temp.m_strName = "stream_sort_bucket";
   temp.init(4);

   Process* myproc = Sector::createJob();

   if (myproc->run(s, temp, "sorthash", 1) < 0)
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
   }

   Stream output;
   output.m_strName = "stream_sort_result";
   output.init(-1);

   if (myproc->run(temp, output, "sort", -1) < 0)
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
