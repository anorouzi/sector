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
m_bLocal(false)
{
}

void PRobot::setCmd(const string& name)
{
   m_strSrc = name;
   m_strCmd = name;
}

void PRobot::setParam(const string& param)
{
   m_strParam = param;
}

void PRobot::setCmdFlag(const bool& local)
{
   m_bLocal = local;
}

int PRobot::generate()
{
   fstream cpp;
   cpp.open((m_strSrc + ".cpp").c_str(), ios::in | ios::out | ios::trunc);

cout << "open " << m_strSrc << endl;

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
   cpp << "   string sfile = string(input->m_pcUnit) + \".result\";" << endl;
   cpp << endl;

   //if (m_bLocal)
   //   m_strCmd = "file->m_strLibDir + \"/\" + " + m_strCmd;

   // Python: .py
   // Perl: .pl

   // system((string("") + FUNC + " " + ifile + " " + PARAM + " > " + ofile).c_str());
   cpp << "   system((string(\"\") + ";
   if (m_bLocal)
      cpp << "file->m_strLibDir + \"/\" + ";
   cpp << "\"";
   cpp << m_strCmd;
   cpp << "\"";
   cpp << " + \" \" + ifile + \" \" + ";
   cpp << "\"";
   cpp << m_strParam;
   cpp << "\"";
   cpp << " + \" > \" + ofile).c_str());" << endl;

   cpp << endl;
   cpp << "   output->m_iRows = 0;" << endl;
   cpp << "   file->m_sstrFiles.insert(sfile);" << endl;
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
   return 0;
}
