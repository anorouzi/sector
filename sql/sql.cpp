#include "sql.h"
#include <iostream>
#include <stack>

int SQLParser::getToken(char** expr, SQLToken& token)
{
   char* p = *expr;

   // skip all blanks
   while (((' ' == *p) || ('\t' == *p)) && ('\0' != *p))
      ++ p;

   token.m_strToken = "";

   if ((',' == *p) || (';' == *p) || ('(' == *p) || (')' == *p))
   {
      token.m_Type = MARK;
      token.m_strToken.append(1, *p++);
      *expr = p;
      return 0;
   }

   if (('+' == *p) || ('-' == *p) || ('*' == *p) || ('/' == *p))
   {
      token.m_Type = ARITH_OP;
      token.m_strToken.append(1, *p++);
      *expr = p;
      return 0;
   }

   if (('>' == *p) || ('<' == *p) || ('=' == *p))
   {
      token.m_Type = BOOL_OP;
      token.m_strToken.append(1, *p++);
      if ('=' == *p)
         token.m_strToken.append(1, *p++);
      *expr = p;
      return 0;
   }

   if (('0' <= *p) && ('9' >= *p))
   {
      token.m_Type = CONSTANT;
      token.m_DataType = INTEGER;

      while (('0' <= *p) && ('9' >= *p))
         token.m_strToken.append(1, *p++);

      if ('.' == *p)
      {
         token.m_strToken.append(1, *p++);

         while (('0' <= *p) && ('9' >= *p))
            token.m_strToken.append(1, *p++);

         token.m_DataType = FLOAT;
      }

      if (('+' != *p) && ('-' != *p) && ('*' != *p) && ('/' != *p) && (')' != *p) && (' ' != *p) && ('\t' != *p) && (';' != *p))
         return -1;

      *expr = p;
      return 0;
   }
   
   if ((('A' <= *p) && ('Z' >= *p)) || (('a' <= *p) && ('z' >= *p)))
   {
      while ((('A' <= *p) && ('Z' >= *p)) || (('a' <= *p) && ('z' >= *p)) || (('0' <= *p) && ('9' >= *p)))
         token.m_strToken.append(1, *p++);

      if (("SELECT" == token.m_strToken) || ("FROM" == token.m_strToken) || ("WHERE" == token.m_strToken))
         token.m_Type = KEYWORD;
      else
         token.m_Type = UNKNOWN;

      *expr = p;
      return 0;
   }

   // constant string 
   if ('\'' == *p)
   {
      while (((' ' == *p) || ('\t' == *p)) && ('\0' != *p))
         token.m_strToken.append(1, *p++);

      if ('\'' != *(p - 1))
         return -1;

      token.m_Type = CONSTANT;
      token.m_DataType = STRING;
   }

   // no match found, grammar error
   return -1;
}

int SQLParser::parse(const string& expr, SQLExpr& sql)
{
   char* p = (char*)expr.c_str();
   SQLToken token;

   // all SQL expression startes with SELECT
   if ((0 != getToken(&p, token)) || ("SELECT" != token.m_strToken))
      return -1;

   // retrieve "field" section
   if ((0 != getToken(&p, token)) || (UNKNOWN != token.m_Type))
      return -1;
   sql.m_vstrFieldList.insert(sql.m_vstrFieldList.end(), token.m_strToken);

   if (0 != getToken(&p, token))
      return -1;

   while ("FROM" != token.m_strToken)
   {
      if ("," != token.m_strToken)
         return -1;

      if ((0 != getToken(&p, token)) || (UNKNOWN != token.m_Type))
         return -1;
      sql.m_vstrFieldList.insert(sql.m_vstrFieldList.end(), token.m_strToken);

      if (0 != getToken(&p, token))
         return -1;
   }

   // FROM "table" list
   if ((0 != getToken(&p, token)) || (UNKNOWN != token.m_Type))
      return -1;
   sql.m_vstrTableList.insert(sql.m_vstrTableList.end(), token.m_strToken);

   if (0 != getToken(&p, token))
      return -1;

   while (("WHERE" != token.m_strToken) && (";" != token.m_strToken))
   {
      if ("," != token.m_strToken)
         return -1;

      if ((0 != getToken(&p, token)) || (UNKNOWN != token.m_Type))
         return -1;
      sql.m_vstrTableList.insert(sql.m_vstrTableList.end(), token.m_strToken);

      if (0 != getToken(&p, token))
         return -1;
   }

   // all SQL expression ends with ";"
   if (";" == token.m_strToken)
      return 0;

   if (0 != getToken(&p, token))
      return -1;

   // WHERE condition
   while (";" != token.m_strToken)
   {
      sql.m_Condition.insert(sql.m_Condition.end(), token);

      if (0 != getToken(&p, token))
         return -1;
   }

   if (";" != token.m_strToken)
      return -1;

cout << "start validating " << sql.m_Condition.size() << endl;

   // validate "condition"
   return validateCond(sql.m_Condition);
}

int SQLParser::validateCond(vector<SQLToken>& cond)
{
   stack<SQLToken> cs;

   for (vector<SQLToken>::iterator i = cond.begin(); i != cond.end(); ++ i)
   {
      char c = *(i->m_strToken.c_str());
      switch (c)
      {
      case '+':
      case '-':
      case '*':
      case '/':
         i->m_Type = ARITH_OP;
         break;

      case '>':
      case '<':
      case '=':
         i->m_Type = BOOL_OP;
         break;

      case '(':
         i->m_Type = MARK;
         i->m_Mark = LEFT;
         break;

      case ')':
         i->m_Type = MARK;
         i->m_Mark = RIGHT;
         break;

      case '\'':
         i->m_Type = CONSTANT;
         i->m_DataType = STRING;
         i->m_strToken = i->m_strToken.substr(1, i->m_strToken.length() - 2);
         break;

      default:
         if (("AND" == i->m_strToken) || ("OR" == i->m_strToken) || ("NOT" == i->m_strToken))
            i->m_Type = BOOL_OP;
         else if (("TRUE" == i->m_strToken) || ("FALSE" == i->m_strToken))
         {
            i->m_Type = CONSTANT;
            i->m_DataType = BOOLEAN;
         }
      }


      if (cs.empty())
         cs.push(*i);
      else if (MARK == i->m_Type)
      {
         if (LEFT == i->m_Mark)
            cs.push(*i);
         else if ((MARK == cs.top().m_Type) && (LEFT == cs.top().m_Mark))
            cs.pop();
         else if (CONSTANT == cs.top().m_Type)
         {
            SQLToken tmp = cs.top();
            cs.pop();
            cs.pop();
            cs.push(tmp);
         }
         else
            return -1;
      }
      else if ((ARITH_OP == i->m_Type) || (BOOL_OP == i->m_Type))
         cs.push(*i);
      else if (CONSTANT == i->m_Type)
      {
         if ("-" == cs.top().m_strToken)
         {
            cs.pop();
            if (CONSTANT != cs.top().m_Type)
            {
               if (ARITH_OP == cs.top().m_Type)
                  cs.pop();
               else
                  cs.push(*i);
            }
         }
         else if ("NOT" == cs.top().m_strToken)
         {
            cs.pop();
            if (BOOL_OP == cs.top().m_Type)
               cs.pop();
            else
               cs.push(*i);
         }
         else if ((ARITH_OP == cs.top().m_Type) || (BOOL_OP == cs.top().m_Type))
         {
            cs.pop();
            if (CONSTANT != cs.top().m_Type)
               return -1;
         }
         else
            return -1;
      }
      else
         return -1;
   }

   if (cs.size() <= 1)
      return 0;
   return -1;
}

int SQLParser::buildTree(vector<SQLToken>& expr, const int& start, const int& end, EvalTree* tree)
{
   if (start == end)
   {
      tree = new EvalTree;
      tree->m_Token = expr[start];
      tree->m_Left = tree->m_Right = NULL;
      return 0;
   }

   list<EvalTree*> subtree;

   int c;
   int s;
   for (int i = start; i != end; ++ i)
   {
      if ((MARK == expr[i].m_Type) && (LEFT == expr[i].m_Mark))
      {
         c = 1;
         s = i;
         while ((i != end) && (c != 0))
         {
             if ((MARK == expr[i].m_Type) && (LEFT == expr[i].m_Mark))
                 c ++;
             if ((MARK == expr[i].m_Type) && (RIGHT == expr[i].m_Mark))
                 c --;
             ++ i;
         }
         EvalTree* t;
         buildTree(expr, s + 1, i - 1, t);
         subtree.push_back(t);
      }
      else
      {
         EvalTree* t = new EvalTree;
         t->m_Token = expr[i];
         t->m_Left = t->m_Right = NULL;
         subtree.push_back(t);
      }
   }

   if (subtree.size() == 1)
   {
       tree = *subtree.begin();
       return 0;
   }

   for (list<EvalTree*>::iterator i = subtree.begin(); i != subtree.end(); ++ i)
   {
      if (ARITH_OP == (*i)->m_Token.m_Type)
         buildArithTree(subtree);
      else if (BOOL_OP == (*i)->m_Token.m_Type)
         buildBoolTree(subtree);
   }

   if (subtree.size() > 1)
      return -1;

   if (!subtree.empty())
      tree = *subtree.begin();

   return 0;
}

int SQLParser::buildArithTree(list<EvalTree*>& expr)
{
   if (expr.empty())
      return 0;

   // check the negative sign (as in -5)
   for (list<EvalTree*>::iterator i = expr.begin(); i != expr.end(); ++ i)
   {
      if ((ARITH_OP == (*i)->m_Token.m_Type) && ("-" == (*i)->m_Token.m_strToken))
      {
         if (i == expr.end())
            return -1;

         list<EvalTree*>::iterator prev = -- i;
         ++ i;

         if ((i == expr.begin()) || (CONSTANT != (*prev)->m_Token.m_Type))
         {
            list<EvalTree*>::iterator tmp = i ++;
            expr.erase(tmp);

            if ((INTEGER == (*i)->m_Token.m_DataType) || (FLOAT == (*i)->m_Token.m_DataType))
               (*i)->m_Token.m_strToken = "-" + (*i)->m_Token.m_strToken;
            else
               return -1;
         }
      }
   }

   // check "*" and "/"
   for (list<EvalTree*>::iterator i = expr.begin(); i != expr.end(); ++ i)
   {
      if (ARITH_OP == (*i)->m_Token.m_Type)
      {
         if (("*" == (*i)->m_Token.m_strToken) || ("/" == (*i)->m_Token.m_strToken))
         {
            list<EvalTree*>::iterator prev = -- i;
            ++ i;
            list<EvalTree*>::iterator next = ++ i;
            -- i;

            (*i)->m_Left = *prev;
            (*i)->m_Right = *next;
            expr.erase(prev);
            expr.erase(next);
         }
      }
   }

   // check "+" and "-"
   for (list<EvalTree*>::iterator i = expr.begin(); i != expr.end(); ++ i)
   {
      if (ARITH_OP == (*i)->m_Token.m_Type)
      {
         if (("+" == (*i)->m_Token.m_strToken) || ("-" == (*i)->m_Token.m_strToken))
         {
            list<EvalTree*>::iterator prev = -- i;
            ++ i;
            list<EvalTree*>::iterator next = ++ i;
            -- i;

            (*i)->m_Left = *(prev);
            (*i)->m_Right = *(next);
            expr.erase(prev);
            expr.erase(next);
         }
      }
   }

   if (expr.size() > 1)
      return -1;

   return 0;
}

int SQLParser::buildBoolTree(list<EvalTree*>& expr)
{
   if (expr.empty())
      return 0;

   // check the NOT logic (as in NOT expr)
   for (list<EvalTree*>::iterator i = expr.begin(); i != expr.end(); ++ i)
   {
      if ((BOOL_OP == (*i)->m_Token.m_Type) && ("NOT" == (*i)->m_Token.m_strToken))
      {
         if (i == expr.end())
            return -1;

         list<EvalTree*>::iterator next = ++ i;
         -- i;

         (*i)->m_Right = *(next);
         expr.erase(next);
      }
   }

   // check the AND logic (as in expr AND expr)
   for (list<EvalTree*>::iterator i = expr.begin(); i != expr.end(); ++ i)
   {
      if ((BOOL_OP == (*i)->m_Token.m_Type) && ("AND" == (*i)->m_Token.m_strToken))
      {
         if ((i == expr.begin()) || (i == expr.end()))
            return -1;

         list<EvalTree*>::iterator prev = -- i;
         ++ i;
         list<EvalTree*>::iterator next = ++ i;
         -- i;

         (*i)->m_Left = *(prev);
         (*i)->m_Right = *(next);
         expr.erase(prev); 
         expr.erase(next);
      }
   }

   // check the OR logic (as in expr OR expr)
   for (list<EvalTree*>::iterator i = expr.begin(); i != expr.end(); ++ i)
   {
      if ((BOOL_OP == (*i)->m_Token.m_Type) && ("OR" == (*i)->m_Token.m_strToken))
      {
         if ((i == expr.begin()) || (i == expr.end()))
            return -1;

         list<EvalTree*>::iterator prev = -- i;
         ++ i;
         list<EvalTree*>::iterator next = ++ i;
         -- i;

         (*i)->m_Left = *(prev);
         (*i)->m_Right = *(next);
         expr.erase(prev);
         expr.erase(next);
      }
   }

   if (expr.size() > 1)
      return -1;

   return 0;
}
