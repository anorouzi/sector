#include "speclient.h"

using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

   vector<string> s;
   s.insert(s.begin(), "stream.dat");

   Process* myproc = Sector::createJob();

   myproc->open(s, "myProc");
   myproc->run();

   while (true)
   {
      char* res;
      int size;
      string file;
      int64_t offset;
      int rows;

      if ((-1 == myproc->read(res, size, file, offset, rows, true)) || (0 == size))
         break;

      cout << "read one block " << size << endl;

      for (int i = 0; i < size; i += 4)
         cout << *(int*)(res + i) << endl;
   }

   myproc->close();
   Sector::releaseJob(myproc);

   Sector::close();

   return 0;
}


// user defined process

//int myProc(const char* unit, const int& size, char* result, int& rsize, const char* param, const int& psize)
//{
//}
