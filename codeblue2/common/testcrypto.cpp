#include "crypto.h"
#include <cstring>
#include <iostream>

using namespace std;

int main()
{
   Crypto encoder, decoder;

   unsigned char key[16];
   unsigned char iv[8];
   Crypto::generateKey(key, iv);

   encoder.initEnc(key, iv);
   decoder.initDec(key, iv);

   //for (int i = 0; i < 16; i++)
   //   printf("%d \t", key[i]);

   const char* text = "hello world!";
   unsigned char hello[4096];
   memcpy(hello, text, strlen(text) + 1);
   unsigned char enc[4096];
   unsigned char dec[4096];

   int len1  = 4096;
   encoder.encrypt(hello, strlen(text) + 1, enc, len1);

   cout << "hoho enc " << len1 << " " << enc << endl;

   int len2 = 4096;
   decoder.decrypt(enc, len1, dec, len2);

   cout << "KK " << dec << endl;

   encoder.release();
   decoder.release();

   return 0;
}
