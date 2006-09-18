%{
   #include <iostream>
   #include "sql.h"
   using namespace std;

   #define YYSTYPE CQType

   int yylex (void);
   void yyerror (char const *);

   static CQType q;
   static char* input;
   static int ilen;
   static int ip;
%}

%token SELECT
%token FROM
%token WHERE

%token T_EQ
%token T_LT
%token T_LE
%token T_GT
%token T_GE
%token T_NE

%token T_INT
%token T_REAL
%token T_STRING
%token T_QSTRING

%left  '+' '-'
%left  '*' '/'

%%

start
   : query ';'
     {q = $1; YYACCEPT;}
   ;

query
   : SELECT list FROM table
     {$$ = CParser::finalizeQuery($2, $4);}

   | SELECT list FROM table WHERE condition
     {$$ = CParser::finalizeQuery($2, $4, $6);}

   ;

table
   : T_STRING
     {$$ = CParser::addTable($1);}
   ;

list
   : '*'
     {$$ = CParser::addAttr($1);}

   | T_STRING
     {$$ = CParser::addAttr($1);}

   | T_STRING ',' list
     {$$ = CParser::addAttr($1, $3);}

   ;

condition
   : arith_expr T_LT arith_expr
     {$$ = CParser::addCond(LT, UNKNOWN, $1, $3);}

   | arith_expr T_LE arith_expr
     {$$ = CParser::addCond(LE, UNKNOWN, $1, $3);}

   | arith_expr T_GT arith_expr
     {$$ = CParser::addCond(GT, UNKNOWN, $1, $3);}

   | arith_expr T_GE arith_expr
     {$$ = CParser::addCond(GE, UNKNOWN, $1, $3);}

   | arith_expr T_EQ arith_expr
     {$$ = CParser::addCond(EQ, UNKNOWN, $1, $3);}

   | arith_expr T_NE arith_expr
     {$$ = CParser::addCond(NE, UNKNOWN, $1, $3);}

   ;

arith_expr
   : T_STRING
     {$$ = CParser::addCond(ATOM, ATTR, $1);}

   | const_value
     {$$ = $1;}

   | arith_expr '+' arith_expr
     {$$ = CParser::addCond(ADD, UNKNOWN, $1, $3);}

   | arith_expr '-' arith_expr
     {$$ = CParser::addCond(SUB, UNKNOWN, $1, $3);}

   | arith_expr '*' arith_expr
     {$$ = CParser::addCond(MUL, UNKNOWN, $1, $3);}

   | arith_expr '/' arith_expr
     {$$ = CParser::addCond(DIV, UNKNOWN, $1, $3);}

   | '(' arith_expr ')'
     {$$ = $2;}
   ;

const_value
   : T_QSTRING
     {$$ = CParser::addCond(ATOM, STRING, $1);}

   | T_INT
     {$$ = CParser::addCond(ATOM, INT, $1);}

   | T_REAL
     {$$ = CParser::addCond(ATOM, FLOAT, $1);}
   ;


%%

CQueryAttr* CParser::parse(const char* qt, const int& len)
{
   input = (char*)qt;
   ilen = len;
   ip = 0;

   int rc = yyparse();

   if (rc < 0)
      return NULL;

   return &(q.m_queryVal);
}

int yylex (void)
{
   while (((input[ip] == ' ') || (input[ip] == '\t')) && (ip < ilen))
      ip ++;

   if (ip == ilen)
      return 0;

   int oip = ip;
   ip ++;
  
   while ((input[ip] != ' ') && (input[ip] != '\t') && (input[ip] != ',') && (input[ip] != ';') && (input[ip] != '*') && (ip < ilen))
      ip ++;

   char str[1024];
   memcpy(str, input + oip, ip - oip);
   str[ip - oip] = '\0';

   yylval.m_strVal = str;

   cout << "yylex: " << str << " " << yylval.m_strVal << endl;

   if (yylval.m_strVal == "SELECT")
      return SELECT;
   else if (yylval.m_strVal == "FROM")
      return FROM;
   else if (yylval.m_strVal == "WHERE")
      return WHERE;
   else if ((ip - oip == 1) && ((str[0] == ';') || (str[0] == ',') || (str[0] == '*')))
      return str[0];
   else
   {
      cout << "T_STRING\n";
      return T_STRING;
   }
}

void yyerror(char const *s) 
{
  std::cerr << "YYError: " << s << std::endl;
}
