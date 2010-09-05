#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include "sphere.h"

#ifdef WIN32
    #ifdef GREP_EXPORTS
        #define GREP_API __declspec(dllexport)
    #else
        #define GREP_API __declspec(dllimport)
    #endif
#else
    #define GREP_API
#endif


using namespace std;

extern "C"
{

int GREP_API grep(const SInput* input, SOutput* output, SFile* file)
{
   string ifile = file->m_strHomeDir + input->m_pcUnit;
   string ofile = ifile + ".result";

#ifndef WIN32
   system((string("") + "grep" + " sphere" +  " " + ifile + " " +  " > " + ofile).c_str());
#else
   string sys_cmd ("FIND \"sphere\" \"" + ifile + "\"" +  " > " + ofile);
   system(sys_cmd.c_str());
#endif

   ifstream dat(ofile.c_str());
   dat.seekg(0, ios::end);
   std::streamoff size = dat.tellg();
   dat.seekg(0);

   output->m_iRows = 1;
   output->m_pllIndex[0] = 0;
   output->m_pllIndex[1] = size + 1;
   dat.read(output->m_pcResult, size);
   output->m_pcResult[size] = '\0';
   dat.close();
   unlink(ofile.c_str());

   return 0;
}

}
