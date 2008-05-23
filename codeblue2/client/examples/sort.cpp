#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <stdint.h>
using namespace std;

extern "C"
{

struct Key
{
   uint32_t v1;
   uint32_t v2;
   uint16_t v3;
};

bool comp(const Key& k1, const Key& k2)
{
   if (k1.v1 < k1.v1)
      return true;
   if (k1.v1 > k1.v1)
      return false;

   if (k1.v2 < k1.v2)
      return true;
   if (k1.v2 > k1.v2)
      return false;

   if (k1.v3 < k1.v3)
      return true;

   return false;
}

struct Record
{
   char v[100];
};

struct ltrec
{
   bool operator()(const Record* r1, const Record* r2) const
   {
      return comp(*(Key*)(r1->v), *(Key*)(r2->v));
   }
};

// hash k into a value in [0, 2^n -1), n < 32
int hash(const Key* k, const int& n)
{
   return (k->v1 >> (32 - n));
}

void sortbucket(const char* bucket)
{
   ifstream ifs(bucket);

   if (ifs.fail())
      return;

   ifs.seekg(0, ios::end);
   int size = ifs.tellg();
   ifs.seekg(0, ios::beg);

   char* rec = new char[size];
   ifs.read(rec, size);
   ifs.close();

   vector<Record*> vr;
   vr.resize(size / 100);
   Record* r = (Record*)rec;
   for (vector<Record*>::iterator i = vr.begin(); i != vr.end(); ++ i)
      *i = r ++;

   sort(vr.begin(), vr.end(), ltrec());

   ofstream ofs(bucket, ios::trunc);
   for (vector<Record*>::iterator i = vr.begin(); i != vr.end(); ++ i)
      ofs.write((char*)*i, 100);
   ofs.close();

   delete [] rec;
}

int sort(const char* unit, const int& rows, const int64_t& index, char* result, int& rsize, int& rrows, int64_t* rindex, int& bid, const char* param, const int& psize)
{
   string input = string(result) + unit;

   cout << "sorting " << input << endl;

   sortbucket(input.c_str());

   rsize = 0;
   bid = 0;

   return 0;
}

/*
int main(int argc, char** argv)
{
   const int n = 6;
   const int B = 64; // 2^6

   ifstream ifs;
   ifs.open(argv[1]);

   ofstream buckets[B];
   char bf[B][64];

   for (int i = 0; i < B; ++ i)
   {
      sprintf(bf[i], "%s.%d", argv[1], i);
      buckets[i].open(bf[i], ios::binary);
   }

   char record[100];
   for (int i = 0; i < 100000000; ++ i)
   {
      ifs.read(record, 100);
      buckets[hash((Key*)record, n)].write(record, 100);
   }

   ifs.close();
   for (int i = 0; i < B; ++ i)
      buckets[i].close();

   for (int i = 0; i < B; ++ i)
      sortbucket(bf[i]);

   return 0;
}
*/

}
