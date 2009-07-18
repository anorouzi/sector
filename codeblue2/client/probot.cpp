/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/


This file is part of Sector Client.

The Sector Client is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

The Sector Client is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 07/16/2009
*****************************************************************************/


#include <fstream>
#include <iostream>
#include <probot.h>

using namespace std;

PRobot::PRobot():
m_strSrc(),
m_strCmd(),
m_strParam(),
m_bLocal(false),
m_strOutput()
{
}

void PRobot::setCmd(const string& name)
{
   m_strSrc = name;
   m_strCmd = name;
}

void PRobot::setParam(const string& param)
{
   m_strParam = "";
   for (char* p = (char*)param.c_str(); *p != '\0'; ++ p)
   {
      if (*p == '"')
         m_strParam.push_back('\\');
      m_strParam.push_back(*p);
   }
}

void PRobot::setCmdFlag(const bool& local)
{
   m_bLocal = local;
}

void PRobot::setOutput(const string& output)
{
   m_strOutput = output;
}

int PRobot::generate()
{
   fstream cpp;
   cpp.open((m_strSrc + ".cpp").c_str(), ios::in | ios::out | ios::trunc);

   cpp << "#include <iostream>" << endl;
   cpp << "#include <fstream>" << endl;
   cpp << "#include <sphere.h>" << endl;
   cpp << endl;
   cpp << "using namespace std;" << endl;
   cpp << endl;
   cpp << "extern \"C\"" << endl;
   cpp << "{" << endl;
   cpp << endl;

   cpp << "int " << m_strCmd << "(const SInput* input, SOutput* output, SFile* file)" << endl;
   cpp << "{" << endl;
   cpp << "   string ifile = file->m_strHomeDir + input->m_pcUnit;" << endl;
   cpp << "   string ofile = ifile + \".result\";" << endl;
   cpp << endl;

   // Python: .py
   // Perl: .pl

   // system((string("") + FUNC + " " + ifile + " " + PARAM + " > " + ofile).c_str());
   cpp << "   system((string(\"\") + ";
   if (m_bLocal)
      cpp << "file->m_strLibDir + \"/\" + ";
   cpp << "\"";
   cpp << m_strCmd;
   cpp << "\" + ";
   cpp << "\" ";
   cpp << m_strParam;
   cpp << "\" + ";
   cpp << " \" \" + ifile + \" \" + ";
   cpp << " \" > \" + ofile).c_str());" << endl;

   cpp << endl;
   if (m_strOutput.length() == 0)
   {
      cpp << "   ifstream dat(ofile.c_str());" << endl;
      cpp << "   dat.seekg(0, ios::end);" << endl;
      cpp << "   int size = dat.tellg();" << endl;
      cpp << "   dat.seekg(0);" << endl;
      cpp << endl;
      cpp << "   output->m_iRows = 1;" << endl;
      cpp << "   output->m_pllIndex[0] = 0;" << endl;
      cpp << "   output->m_pllIndex[1] = size + 1;" << endl;
      cpp << "   dat.read(output->m_pcResult, size);" << endl;
      cpp << "   output->m_pcResult[size] = '\\0';" << endl;
      cpp << "   dat.close();" << endl;
      cpp << "   unlink(ofile.c_str());" << endl;
   }
   else
   {
      cpp << "   output->m_iRows = 0;" << endl;
      cpp << endl;
      cpp << "   string sfile;" << endl;
      cpp << "   for (int i = 1, n = strlen(input->m_pcUnit); i < n; ++ i)" << endl;
      cpp << "   {" << endl;
      cpp << "      if (input->m_pcUnit[i] == '/')" << endl;
      cpp << "         sfile.push_back('.');" << endl;
      cpp << "      else" << endl;
      cpp << "         sfile.push_back(input->m_pcUnit[i]);" << endl;
      cpp << "   }" << endl;
      cpp << "   sfile = string(" << "\"" << m_strOutput << "\")" << " + \"/\" + sfile;" << endl;
      cpp << endl;
      cpp << "   system((\"mkdir -p \" + file->m_strHomeDir + " << "\"" << m_strOutput << "\"" << ").c_str());" << endl;
      cpp << "   system((\"mv \" + ofile + \" \" + file->m_strHomeDir + sfile).c_str());" << endl;
      cpp << endl;
      cpp << "   file->m_sstrFiles.insert(sfile);" << endl;
   }
   cpp << endl;

   cpp << "   return 0;" << endl;
   cpp << "}" << endl;
   cpp << endl;
   cpp << "}" << endl;

   cpp.close();

   return 0;
}

int PRobot::compile()
{
   string CCFLAGS = "-I../ -I../../udt -I../../gmp -I../../common -I../../security";
   system(("g++ " + CCFLAGS + " -shared -fPIC -O3 -o " + m_strSrc + ".so -lstdc++ " + m_strSrc + ".cpp").c_str());

   system(("mv " + m_strSrc + ".* /tmp").c_str());

   return 0;
}
