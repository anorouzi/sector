#ifndef __DHASH_H__
#define __DHASH_H__

#include <openssl/sha.h>
#include <algorithm>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>

class CDHash
{
public:
   CDHash(): m_im(32) {}
   CDHash(const int m): m_im(m) {}
   ~CDHash() {}

   unsigned int hash(const char* str)
   {
      unsigned char res[SHA_DIGEST_LENGTH];

      SHA1((const unsigned char*)str, strlen(str), res);

      return *(unsigned int*)(res + SHA_DIGEST_LENGTH - 4);
   }

   unsigned int hash(const in_addr& ip)
   {
      char tmp[16];

      return hash(inet_ntop(AF_INET, &ip, tmp, 16));
   }

   static unsigned int hash(const char* str, int m)
   {
      unsigned char res[SHA_DIGEST_LENGTH];

      SHA1((const unsigned char*)str, strlen(str), res);

      return (*(unsigned int*)(res + SHA_DIGEST_LENGTH - 4)) & (int(pow(2, double(m))) - 1);
   }

   static unsigned int hash(const in_addr& ip, int m)
   {
      char tmp[16];

      return CDHash::hash(inet_ntop(AF_INET, &ip, tmp, 16), m);
   }

private:
   unsigned int m_im;
};

#endif
