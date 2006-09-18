#ifndef __QUERY_H__
#define __QUERY_H__

#include <vector>
#include <string>
#include <iostream>

using namespace std;

enum OP {ATOM, EQ, LT, LE, GT, GE, NE, AND, OR, NOT, ADD, SUB, MUL, DIV, NEG, EXP};
enum OT {UNKNOWN, ATTR, STRING, INT, FLOAT};

class CCond
{
public:
   CCond()
   {
   }

   ~CCond()
   {
   }

public:
   bool evaluate()
   {
      switch (m_OP)
      {
      case EQ:
         //return m_pLeft->evaluate() == m_pRight->evaluate();

      case LT:
         //return m_pLeft->evaluate() < m_pRight->evaluate();

      case LE:
         //return m_pLeft->evaluate() <= m_pRight->evaluate();

      case GT:
         //return m_pLeft->evaluate() > m_pRight->evaluate();

      case GE:
         //return m_pLeft->evaluate() >= m_pRight->evaluate();

      case NE:
         //return m_pLeft->evaluate() != m_pRight->evaluate();

      default:
         // error, throw exception
         return true;
      }

      return true;
   }

public:
   OP m_OP;
   CCond* m_pLeft;
   CCond* m_pRight;

   OT m_OT;

   string m_strVal;
   int m_intVal;
   float m_floatVal;

   string m_strAttr;
};

struct CQueryAttr
{
   vector<string> m_vTableList;
   vector<string> m_vAttrList;
   CCond m_Cond;
};

struct CQType
{
   string m_strVal;
   vector<string> m_vstrVal;
   CQueryAttr m_queryVal;
   CCond m_Cond;
};

class CParser
{
public:
   static CQType addTable(const CQType& t)
   {
      cout << "add table " << t.m_strVal << endl;

      CQType al;
      al.m_vstrVal.insert(al.m_vstrVal.end(), t.m_strVal);

      return al;
   }

   static CQType addAttr(const CQType& a)
   {
      cout << "add attr " << " " << a.m_strVal << endl;

      CQType al;
      al.m_vstrVal.insert(al.m_vstrVal.end(), a.m_strVal);

      return al;
   }
   
   static CQType addAttr(const CQType& a, CQType& al)
   {
      cout << "add attr " << " " << a.m_strVal << " " << al.m_vstrVal.size() << endl;

      al.m_vstrVal.insert(al.m_vstrVal.end(), a.m_strVal);

      return al;
   }

   static CQType finalizeQuery(const CQType& attrlist, const CQType& table)
   {
      cout << "finalize " << table.m_vstrVal.size() << " " << attrlist.m_vstrVal.size() << endl;

      CQType q;
      q.m_queryVal.m_vTableList = table.m_vstrVal;
      q.m_queryVal.m_vAttrList = attrlist.m_vstrVal;

      return q;
   }

   static CQType finalizeQuery(const CQType& attrlist, const CQType& table, const CQType& cond)
   {
      cout << "finalize " << table.m_vstrVal.size() << " " << attrlist.m_vstrVal.size() << endl;

      CQType q;
      q.m_queryVal.m_vTableList = table.m_vstrVal;
      q.m_queryVal.m_vAttrList = attrlist.m_vstrVal;
      q.m_queryVal.m_Cond = cond.m_Cond;

      return q;
   }

   static CQType addCond(const OP& op, const OT& ot, const CQType& expr)
   {
      CQType q;
      q.m_Cond.m_OP = op;
      q.m_Cond.m_OT = ot;
      q.m_Cond.m_pLeft = new CCond;
      q.m_Cond.m_pLeft->m_OP = ATOM;

      if (op == ATOM)
      {
         switch (ot)
         {
         case ATTR:
            q.m_Cond.m_strAttr = expr.m_strVal;
            break;

         case STRING:
            q.m_Cond.m_strVal = expr.m_strVal;
            break;

         case INT:
            q.m_Cond.m_intVal = atoi(expr.m_strVal.c_str());
            break;

         case FLOAT:
            q.m_Cond.m_floatVal = atof(expr.m_strVal.c_str());
            break;

         default:
            // error, throw exception
            break;
         }
      }

      q.m_Cond.m_pRight = new CCond (expr.m_Cond);

      return q;
   }

   static CQType addCond(const OP& op, const OT& ot, const CQType& left, const CQType& right)
   {
      CQType q;
      q.m_Cond.m_OP = op;
      q.m_Cond.m_OT = ot;
      q.m_Cond.m_pLeft = new CCond(left.m_Cond);
      q.m_Cond.m_pRight = new CCond(right.m_Cond);

      return q;
   }

   static CQueryAttr* parse(const char* qt, const int& len);
};

#endif
