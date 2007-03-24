/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#ifndef __SQL_H__
#define __SQL_H__

#include <data.h>
#include <string>
#include <list>

using namespace std;

// simple SQL semnatics
// SELECT * FROM table;
// SELECT field1, field2, ... FROM table;
// SELECT * FROM table WHERE condition;
// SELECT * FROM table1, table2, ... WHERE condition;

namespace cb
{

enum SQL_TokenType {UNKNOWN, KEYWORD, CONSTANT, ARITH_OP, BOOL_OP, MARK};
enum SQL_KeyWord {SELECT, FROM, WHERE};
enum SQL_ARITH_OP {ADD, SUB, MUL, DIV, NEG};
enum SQL_BOOL_OP {AND, OR, NOT, EQ, LT, LE, GT, GE, NE};
enum SQL_MARK {COMMA, SEMICOLON, LEFT, RIGHT};

struct SQLToken
{
   SQL_TokenType m_Type;
   union
   {
      SQL_KeyWord m_KeyWord;
      SQL_ARITH_OP m_Arith_OP;
      SQL_BOOL_OP m_Bool_OP;
      DataType m_DataType;
      SQL_MARK m_Mark;
   };
   string m_strToken;
};

struct SQLExpr
{
   vector<string> m_vstrFieldList;
   vector<string> m_vstrTableList;
   vector<SQLToken> m_Condition;
};

struct EvalTree
{
   SQLToken m_Token;
   EvalTree* m_Left;
   EvalTree* m_Right;
};

class SQLParser
{
public:
   static int parse(const string& expr, SQLExpr& sql);
   static EvalTree* buildTree(vector<SQLToken>& expr, const int& start, const int& end);
   static void releaseTree(EvalTree* tree) {}

private:
   static int getToken(char** expr, SQLToken& token);
   static int validateCond(vector<SQLToken>& cond);

   static int buildArithTree(list<EvalTree*>& expr);
   static int buildBoolTree(list<EvalTree*>& expr);
};

}; // namespace

#endif
