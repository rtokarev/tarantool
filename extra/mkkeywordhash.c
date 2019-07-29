/*
** Compile and run this standalone program in order to generate code that
** implements a function that will translate alphabetic identifiers into
** parser token codes.
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdbool.h>

/*
** A header comment placed at the beginning of generated code.
*/
static const char zHdr[] =
  "/***** This file contains automatically generated code ******\n"
  "**\n"
  "** The code in this file has been automatically generated by\n"
  "**\n"
  "**   extra/mkkeywordhash.c\n"
  "**\n"
  "** The code in this file implements a function that determines whether\n"
  "** or not a given identifier is really an SQL keyword.  The same thing\n"
  "** might be implemented more directly using a hand-written hash table.\n"
  "** But by using this automatically generated code, the size of the code\n"
  "** is substantially reduced.  This is important for embedded applications\n"
  "** on platforms with limited memory.\n"
  "*/\n"
;

/*
** All the keywords of the SQL language are stored in a hash
** table composed of instances of the following structure.
*/
typedef struct Keyword Keyword;
struct Keyword {
  char *zName;         /* The keyword name */
  char *zTokenType;    /* Token value for this keyword */
  int mask;            /* Code this keyword if non-zero */
  bool isReserved;     /* Is this word reserved by SQL standard */
  int id;              /* Unique ID for this record */
  int hash;            /* Hash on the keyword */
  int offset;          /* Offset to start of name string */
  int len;             /* Length of this keyword, not counting final \000 */
  int prefix;          /* Number of characters in prefix */
  int longestSuffix;   /* Longest suffix that is a prefix on another word */
  int iNext;           /* Index in aKeywordTable[] of next with same hash */
  int substrId;        /* Id to another keyword this keyword is embedded in */
  int substrOffset;    /* Offset into substrId for start of this keyword */
  char zOrigName[50];  /* Original keyword name before processing */
};

/*
** Define masks used to determine which keywords are allowed
*/
#define ALTER        0x00000001
#define ALWAYS       0x00000002
#  define AUTOINCR   0x00000010
#  define CAST       0x00000020
#  define COMPOUND   0x00000040
#  define CONFLICT   0x00000080
#define EXPLAIN      0x00000100
#define FKEY         0x00000200
#  define PRAGMA     0x00000400
#define SUBQUERY     0x00001000
#  define TRIGGER    0x00002000
#  define VIEW       0x00008000
#  define CTE        0x00040000
#  define RESERVED   0x00000001
/*
** These are the keywords
*/
static Keyword aKeywordTable[] = {
  { "ABORT",                  "TK_ABORT",       CONFLICT|TRIGGER, false },
  { "ACTION",                 "TK_ACTION",      FKEY,             false },
  { "ADD",                    "TK_ADD",         ALTER,            false },
  { "AFTER",                  "TK_AFTER",       TRIGGER,          false },
  { "ALL",                    "TK_ALL",         ALWAYS,           true  },
  { "ALTER",                  "TK_ALTER",       ALTER,            true  },
  { "ANALYZE",                "TK_STANDARD",    RESERVED,         true  },
  { "AND",                    "TK_AND",         ALWAYS,           true  },
  { "AS",                     "TK_AS",          ALWAYS,           true  },
  { "ASC",                    "TK_ASC",         ALWAYS,           true  },
  { "AUTOINCREMENT",          "TK_AUTOINCR",    AUTOINCR,         false },
  { "BEFORE",                 "TK_BEFORE",      TRIGGER,          false },
  { "BEGIN",                  "TK_BEGIN",       TRIGGER,          true  },
  { "BETWEEN",                "TK_BETWEEN",     ALWAYS,           true  },
  { "BOOL",                   "TK_BOOLEAN",     ALWAYS,           true  },
  { "BOOLEAN",                "TK_BOOLEAN",     ALWAYS,           true  },
  { "BY",                     "TK_BY",          ALWAYS,           true  },
  { "CASCADE",                "TK_CASCADE",     FKEY,             false },
  { "CASE",                   "TK_CASE",        ALWAYS,           true  },
  { "CAST",                   "TK_CAST",        CAST,             false },
  { "CHECK",                  "TK_CHECK",       ALWAYS,           true  },
  { "COLLATE",                "TK_COLLATE",     ALWAYS,           true  },
  /* gh-3075: Reserved until ALTER ADD COLUMN is implemeneted.
   * Move it back to ALTER when done.
   */
  /* { "COLUMN",                 "TK_COLUMNKW",    ALTER,            true  }, */
  { "COLUMN",                 "TK_STANDARD",    RESERVED,         true  },
  { "COMMIT",                 "TK_COMMIT",      ALWAYS,           true  },
  { "CONFLICT",               "TK_CONFLICT",    CONFLICT,         false },
  { "CONSTRAINT",             "TK_CONSTRAINT",  ALWAYS,           true  },
  { "CREATE",                 "TK_CREATE",      ALWAYS,           true  },
  { "CROSS",                  "TK_JOIN_KW",     ALWAYS,           true  },
  { "DEFAULT",                "TK_DEFAULT",     ALWAYS,           true  },
  { "DEFERRED",               "TK_DEFERRED",    ALWAYS,           false },
  { "DEFERRABLE",             "TK_DEFERRABLE",  FKEY,             false },
  { "DELETE",                 "TK_DELETE",      ALWAYS,           true  },
  { "DESC",                   "TK_DESC",        ALWAYS,           true  },
  { "DISTINCT",               "TK_DISTINCT",    ALWAYS,           true  },
  { "DROP",                   "TK_DROP",        ALWAYS,           true  },
  { "END",                    "TK_END",         ALWAYS,           true  },
  { "EACH",                   "TK_EACH",        TRIGGER,          true  },
  { "ELSE",                   "TK_ELSE",        ALWAYS,           true  },
  { "ESCAPE",                 "TK_ESCAPE",      ALWAYS,           true  },
  { "EXCEPT",                 "TK_EXCEPT",      COMPOUND,         true  },
  { "EXISTS",                 "TK_EXISTS",      ALWAYS,           true  },
  { "EXPLAIN",                "TK_EXPLAIN",     EXPLAIN,          true  },
  { "FAIL",                   "TK_FAIL",        CONFLICT|TRIGGER, false },
  { "FALSE",                  "TK_FALSE",       ALWAYS,           true  },
  { "FOR",                    "TK_FOR",         TRIGGER,          true  },
  { "FOREIGN",                "TK_FOREIGN",     FKEY,             true  },
  { "FROM",                   "TK_FROM",        ALWAYS,           true  },
  { "FULL",                   "TK_FULL",        ALWAYS,           true  },
  { "GROUP",                  "TK_GROUP",       ALWAYS,           true  },
  { "HAVING",                 "TK_HAVING",      ALWAYS,           true  },
  { "IF",                     "TK_IF",          ALWAYS,           true  },
  { "IGNORE",                 "TK_IGNORE",      CONFLICT|TRIGGER, false },
  { "IMMEDIATE",              "TK_IMMEDIATE",   ALWAYS,           true  },
  { "IN",                     "TK_IN",          ALWAYS,           true  },
  { "INDEX",                  "TK_INDEX",       ALWAYS,           true  },
  { "INDEXED",                "TK_INDEXED",     ALWAYS,           false },
  { "INITIALLY",              "TK_INITIALLY",   FKEY,             false },
  { "INNER",                  "TK_JOIN_KW",     ALWAYS,           true  },
  { "INSERT",                 "TK_INSERT",      ALWAYS,           true  },
  { "INSTEAD",                "TK_INSTEAD",     TRIGGER,          false },
  { "INTERSECT",              "TK_INTERSECT",   COMPOUND,         true  },
  { "INTO",                   "TK_INTO",        ALWAYS,           true  },
  { "IS",                     "TK_IS",          ALWAYS,           true  },
  { "JOIN",                   "TK_JOIN",        ALWAYS,           true  },
  { "KEY",                    "TK_KEY",         ALWAYS,           false },
  { "LEFT",                   "TK_JOIN_KW",     ALWAYS,           true  },
  { "LIKE",                   "TK_LIKE_KW",     ALWAYS,           true  },
  { "LIMIT",                  "TK_LIMIT",       ALWAYS,           false },
  { "MATCH",                  "TK_MATCH",       ALWAYS,           true  },
  { "NATURAL",                "TK_JOIN_KW",     ALWAYS,           true  },
  { "NO",                     "TK_NO",          FKEY,             false },
  { "NOT",                    "TK_NOT",         ALWAYS,           true  },
  { "NULL",                   "TK_NULL",        ALWAYS,           true  },
  { "OF",                     "TK_OF",          ALWAYS,           true  },
  { "OFFSET",                 "TK_OFFSET",      ALWAYS,           false },
  { "ON",                     "TK_ON",          ALWAYS,           true  },
  { "OR",                     "TK_OR",          ALWAYS,           true  },
  { "ORDER",                  "TK_ORDER",       ALWAYS,           true  },
  { "OUTER",                  "TK_JOIN_KW",     ALWAYS,           true  },
  { "PARTIAL",                "TK_PARTIAL",     ALWAYS,           true  },
  { "PLAN",                   "TK_PLAN",        EXPLAIN,          false },
  { "PRAGMA",                 "TK_PRAGMA",      PRAGMA,           true  },
  { "PRIMARY",                "TK_PRIMARY",     ALWAYS,           true  },
  { "QUERY",                  "TK_QUERY",       EXPLAIN,          false },
  { "RAISE",                  "TK_RAISE",       TRIGGER,          false },
  { "RECURSIVE",              "TK_RECURSIVE",   CTE,              true  },
  { "REFERENCES",             "TK_REFERENCES",  FKEY,             true  },
  { "REGEXP",                 "TK_LIKE_KW",     ALWAYS,           false },
  { "RELEASE",                "TK_RELEASE",     ALWAYS,           true  },
  { "RENAME",                 "TK_RENAME",      ALTER,            true  },
  { "REPLACE",                "TK_REPLACE",     CONFLICT,         true  },
  { "RESTRICT",               "TK_RESTRICT",    FKEY,             false },
  { "RIGHT",                  "TK_JOIN_KW",     ALWAYS,           true  },
  { "ROLLBACK",               "TK_ROLLBACK",    ALWAYS,           true  },
  { "ROW",                    "TK_ROW",         TRIGGER,          true  },
  { "SAVEPOINT",              "TK_SAVEPOINT",   ALWAYS,           true  },
  { "SCALAR",                 "TK_SCALAR",      ALWAYS,           true  },
  { "SELECT",                 "TK_SELECT",      ALWAYS,           true  },
  { "SET",                    "TK_SET",         ALWAYS,           true  },
  { "SIMPLE",                 "TK_SIMPLE",      ALWAYS,           true  },
  { "START",                  "TK_START",       ALWAYS,           true  },
  { "STRING",                 "TK_STRING_KW",   ALWAYS,           true  },
  { "TABLE",                  "TK_TABLE",       ALWAYS,           true  },
  { "THEN",                   "TK_THEN",        ALWAYS,           true  },
  { "TO",                     "TK_TO",          ALWAYS,           true  },
  { "TRANSACTION",            "TK_TRANSACTION", ALWAYS,           true  },
  { "TRIGGER",                "TK_TRIGGER",     TRIGGER,          true  },
  { "TRUE",                   "TK_TRUE",        ALWAYS,           true  },
  { "UNION",                  "TK_UNION",       COMPOUND,         true  },
  { "UNIQUE",                 "TK_UNIQUE",      ALWAYS,           true  },
  { "UNKNOWN",                "TK_NULL",        ALWAYS,           true  },
  { "UNSIGNED",               "TK_UNSIGNED",    ALWAYS,           true  },
  { "UPDATE",                 "TK_UPDATE",      ALWAYS,           true  },
  { "USING",                  "TK_USING",       ALWAYS,           true  },
  { "VALUES",                 "TK_VALUES",      ALWAYS,           true  },
  { "VIEW",                   "TK_VIEW",        VIEW,             true  },
  { "WITH",                   "TK_WITH",        CTE,              true  },
  { "WHEN",                   "TK_WHEN",        ALWAYS,           true  },
  { "WHERE",                  "TK_WHERE",       ALWAYS,           true  },
  { "ANY",                    "TK_STANDARD",    RESERVED,         true  },
  { "ASENSITIVE",             "TK_STANDARD",    RESERVED,         true  },
  { "BLOB",                   "TK_STANDARD",    RESERVED,         true  },
  { "BINARY",                 "TK_ID",          RESERVED,         true  },
  { "CALL",                   "TK_STANDARD",    RESERVED,         true  },
  { "CHAR",                   "TK_CHAR",        RESERVED,         true  },
  { "CHARACTER",              "TK_ID",          RESERVED,         true  },
  { "CONDITION",              "TK_STANDARD",    RESERVED,         true  },
  { "CONNECT",                "TK_STANDARD",    RESERVED,         true  },
  { "CURRENT",                "TK_STANDARD",    RESERVED,         true  },
  { "CURRENT_USER",           "TK_STANDARD",    RESERVED,         true  },
  { "CURSOR",                 "TK_STANDARD",    RESERVED,         true  },
  { "CURRENT_DATE",           "TK_STANDARD",    RESERVED,         true  },
  { "CURRENT_TIME",           "TK_STANDARD",    RESERVED,         true  },
  { "CURRENT_TIMESTAMP",      "TK_STANDARD",    RESERVED,         true  },
  { "DATE",                   "TK_STANDARD",    RESERVED,         true  },
  { "DATETIME",               "TK_STANDARD",    RESERVED,         true  },
  { "DECIMAL",                "TK_STANDARD",    RESERVED,         true  },
  { "DECLARE",                "TK_STANDARD",    RESERVED,         true  },
  { "DENSE_RANK",             "TK_STANDARD",    RESERVED,         true  },
  { "DESCRIBE",               "TK_STANDARD",    RESERVED,         true  },
  { "DETERMINISTIC",          "TK_STANDARD",    RESERVED,         true  },
  { "DOUBLE",                 "TK_DOUBLE",      RESERVED,         true  },
  { "ELSEIF",                 "TK_STANDARD",    RESERVED,         true  },
  { "FETCH",                  "TK_STANDARD",    RESERVED,         true  },
  { "FLOAT",                  "TK_FLOAT_KW",    RESERVED,         true  },
  { "FUNCTION",               "TK_STANDARD",    RESERVED,         true  },
  { "GET",                    "TK_STANDARD",    RESERVED,         true  },
  { "GRANT",                  "TK_STANDARD",    RESERVED,         true  },
  { "INT",                    "TK_INT",         RESERVED,         true  },
  { "INTEGER",                "TK_INTEGER_KW",  RESERVED,         true  },
  { "INOUT",                  "TK_STANDARD",    RESERVED,         true  },
  { "INSENSITIVE",            "TK_STANDARD",    RESERVED,         true  },
  { "ITERATE",                "TK_STANDARD",    RESERVED,         true  },
  { "LEAVE",                  "TK_STANDARD",    RESERVED,         true  },
  { "LOCALTIME",              "TK_STANDARD",    RESERVED,         true  },
  { "LOCALTIMESTAMP",         "TK_STANDARD",    RESERVED,         true  },
  { "LOOP",                   "TK_STANDARD",    RESERVED,         true  },
  { "NUM",                    "TK_STANDARD",    RESERVED,         true  },
  { "NUMERIC",                "TK_STANDARD",    RESERVED,         true  },
  { "OUT",                    "TK_STANDARD",    RESERVED,         true  },
  { "OVER",                   "TK_STANDARD",    RESERVED,         true  },
  { "PARTITION",              "TK_STANDARD",    RESERVED,         true  },
  { "PRECISION",              "TK_STANDARD",    RESERVED,         true  },
  { "PROCEDURE",              "TK_STANDARD",    RESERVED,         true  },
  { "RANGE",                  "TK_STANDARD",    RESERVED,         true  },
  { "RANK",                   "TK_STANDARD",    RESERVED,         true  },
  { "READS",                  "TK_STANDARD",    RESERVED,         true  },
  { "REAL",                   "TK_REAL",        RESERVED,         true  },
  { "REPEAT",                 "TK_STANDARD",    RESERVED,         true  },
  { "RESIGNAL",               "TK_STANDARD",    RESERVED,         true  },
  { "RETURN",                 "TK_STANDARD",    RESERVED,         true  },
  { "REVOKE",                 "TK_STANDARD",    RESERVED,         true  },
  { "ROWS",                   "TK_STANDARD",    RESERVED,         true  },
  { "ROW_NUMBER",             "TK_STANDARD",    RESERVED,         true  },
  { "SENSITIVE",              "TK_STANDARD",    RESERVED,         true  },
  { "SIGNAL",                 "TK_STANDARD",    RESERVED,         true  },
  { "SMALLINT",               "TK_ID",          RESERVED,         true  },
  { "SPECIFIC",               "TK_STANDARD",    RESERVED,         true  },
  { "SYSTEM",                 "TK_STANDARD",    RESERVED,         true  },
  { "SQL",                    "TK_STANDARD",    RESERVED,         true  },
  { "USER",                   "TK_STANDARD",    RESERVED,         true  },
  { "VARCHAR",                "TK_VARCHAR",     RESERVED,         true  },
  { "WHENEVER",               "TK_STANDARD",    RESERVED,         true  },
  { "WHILE",                  "TK_STANDARD",    RESERVED,         true  },
  { "TEXT",                   "TK_TEXT",        RESERVED,         true  },
  { "TRUNCATE",               "TK_TRUNCATE",    ALWAYS,           true  },
  { "TRIM",                   "TK_TRIM",        ALWAYS,           true  },
  { "LEADING",                "TK_LEADING",     ALWAYS,           true  },
  { "TRAILING",               "TK_TRAILING",    ALWAYS,           true  },
  { "BOTH",                   "TK_BOTH",        ALWAYS,           true  },
};

/* Number of keywords */
static int nKeyword = (sizeof(aKeywordTable)/sizeof(aKeywordTable[0]));

/* Map all alphabetic characters into lower-case for hashing.  This is
** only valid for alphabetics.  In particular it does not work for '_'
** and so the hash cannot be on a keyword position that might be an '_'.
*/
#define charMap(X)   (0x20|(X))

/*
** Comparision function for two Keyword records
*/
static int keywordCompare1(const void *a, const void *b){
  const Keyword *pA = (Keyword*)a;
  const Keyword *pB = (Keyword*)b;
  int n = pA->len - pB->len;
  if( n==0 ){
    n = strcmp(pA->zName, pB->zName);
  }
  assert( n!=0 );
  return n;
}
static int keywordCompare2(const void *a, const void *b){
  const Keyword *pA = (Keyword*)a;
  const Keyword *pB = (Keyword*)b;
  int n = pB->longestSuffix - pA->longestSuffix;
  if( n==0 ){
    n = strcmp(pA->zName, pB->zName);
  }
  assert( n!=0 );
  return n;
}
static int keywordCompare3(const void *a, const void *b){
  const Keyword *pA = (Keyword*)a;
  const Keyword *pB = (Keyword*)b;
  int n = pA->offset - pB->offset;
  if( n==0 ) n = pB->id - pA->id;
  assert( n!=0 );
  return n;
}

/*
** Return a KeywordTable entry with the given id
*/
static Keyword *findById(int id){
  int i;
  for(i=0; i<nKeyword; i++){
    if( aKeywordTable[i].id==id ) break;
  }
  return &aKeywordTable[i];
}

/*
** This routine does the work.  The generated code is printed on standard
** output.
*/
int main(int argc, char **argv){
  int i, j, k, h;
  int bestSize, bestCount;
  int count;
  int nChar;
  int totalLen = 0;
  int aHash[1000];  /* 1000 is much bigger than nKeyword */
  char zText[2000];

  /* Remove entries from the list of keywords that have mask==0 */
  for(i=j=0; i<nKeyword; i++){
    if( aKeywordTable[i].mask==0 ) continue;
    if( j<i ){
      aKeywordTable[j] = aKeywordTable[i];
    }
    j++;
  }
  nKeyword = j;

  /* Fill in the lengths of strings and hashes for all entries. */
  for(i=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    p->len = (int)strlen(p->zName);
    assert( p->len<sizeof(p->zOrigName) );
    memcpy(p->zOrigName, p->zName, p->len+1);
    totalLen += p->len;
    p->hash = (charMap(p->zName[0])*4) ^
              (charMap(p->zName[p->len-1])*3) ^ (p->len*1);
    p->id = i+1;
  }

  /* Sort the table from shortest to longest keyword */
  qsort(aKeywordTable, nKeyword, sizeof(aKeywordTable[0]), keywordCompare1);

  /* Look for short keywords embedded in longer keywords */
  for(i=nKeyword-2; i>=0; i--){
    Keyword *p = &aKeywordTable[i];
    for(j=nKeyword-1; j>i && p->substrId==0; j--){
      Keyword *pOther = &aKeywordTable[j];
      if( pOther->substrId ) continue;
      if( pOther->len<=p->len ) continue;
      for(k=0; k<=pOther->len-p->len; k++){
        if( memcmp(p->zName, &pOther->zName[k], p->len)==0 ){
          p->substrId = pOther->id;
          p->substrOffset = k;
          break;
        }
      }
    }
  }

  /* Compute the longestSuffix value for every word */
  for(i=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    if( p->substrId ) continue;
    for(j=0; j<nKeyword; j++){
      Keyword *pOther;
      if( j==i ) continue;
      pOther = &aKeywordTable[j];
      if( pOther->substrId ) continue;
      for(k=p->longestSuffix+1; k<p->len && k<pOther->len; k++){
        if( memcmp(&p->zName[p->len-k], pOther->zName, k)==0 ){
          p->longestSuffix = k;
        }
      }
    }
  }

  /* Sort the table into reverse order by length */
  qsort(aKeywordTable, nKeyword, sizeof(aKeywordTable[0]), keywordCompare2);

  /* Fill in the offset for all entries */
  nChar = 0;
  for(i=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    if( p->offset>0 || p->substrId ) continue;
    p->offset = nChar;
    nChar += p->len;
    for(k=p->len-1; k>=1; k--){
      for(j=i+1; j<nKeyword; j++){
        Keyword *pOther = &aKeywordTable[j];
        if( pOther->offset>0 || pOther->substrId ) continue;
        if( pOther->len<=k ) continue;
        if( memcmp(&p->zName[p->len-k], pOther->zName, k)==0 ){
          p = pOther;
          p->offset = nChar - k;
          nChar = p->offset + p->len;
          p->zName += k;
          p->len -= k;
          p->prefix = k;
          j = i;
          k = p->len;
        }
      }
    }
  }
  for(i=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    if( p->substrId ){
      p->offset = findById(p->substrId)->offset + p->substrOffset;
    }
  }

  /* Sort the table by offset */
  qsort(aKeywordTable, nKeyword, sizeof(aKeywordTable[0]), keywordCompare3);

  /* Figure out how big to make the hash table in order to minimize the
  ** number of collisions */
  bestSize = nKeyword;
  bestCount = nKeyword*nKeyword;
  for(i=nKeyword/2; i<=2*nKeyword; i++){
    for(j=0; j<i; j++) aHash[j] = 0;
    for(j=0; j<nKeyword; j++){
      h = aKeywordTable[j].hash % i;
      aHash[h] *= 2;
      aHash[h]++;
    }
    for(j=count=0; j<i; j++) count += aHash[j];
    if( count<bestCount ){
      bestCount = count;
      bestSize = i;
    }
  }

  /* Compute the hash */
  for(i=0; i<bestSize; i++) aHash[i] = 0;
  for(i=0; i<nKeyword; i++){
    h = aKeywordTable[i].hash % bestSize;
    aKeywordTable[i].iNext = aHash[h];
    aHash[h] = i+1;
  }

  /* Begin generating code */
  printf("%s", zHdr);
  printf("/* Hash score: %d */\n", bestCount);
  printf("static int keywordCode(const char *z, int n, int *pType, "
         "bool *pFlag){\n");
  printf("  /* zText[] encodes %d bytes of keywords in %d bytes */\n",
          totalLen + nKeyword, nChar+1 );
  for(i=j=k=0; i<nKeyword; i++){
    Keyword *p = &aKeywordTable[i];
    if( p->substrId ) continue;
    memcpy(&zText[k], p->zName, p->len);
    k += p->len;
    if( j+p->len>70 ){
      printf("%*s */\n", 74-j, "");
      j = 0;
    }
    if( j==0 ){
      printf("  /*   ");
      j = 8;
    }
    printf("%s", p->zName);
    j += p->len;
  }
  if( j>0 ){
    printf("%*s */\n", 74-j, "");
  }
  printf("  static const char zText[%d] = {\n", nChar);
  zText[nChar] = 0;
  for(i=j=0; i<k; i++){
    if( j==0 ){
      printf("    ");
    }
    if( zText[i]==0 ){
      printf("0");
    }else{
      printf("'%c',", zText[i]);
    }
    j += 4;
    if( j>68 ){
      printf("\n");
      j = 0;
    }
  }
  if( j>0 ) printf("\n");
  printf("  };\n");

  printf("  static const unsigned short aHash[%d] = {\n", bestSize);
  for(i=j=0; i<bestSize; i++){
    if( j==0 ) printf("    ");
    printf(" %3d,", aHash[i]);
    j++;
    if( j>12 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s  };\n", j==0 ? "" : "\n");

  printf("  static const unsigned short aNext[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    if( j==0 ) printf("    ");
    printf(" %3d,", aKeywordTable[i].iNext);
    j++;
    if( j>12 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s  };\n", j==0 ? "" : "\n");

  printf("  static const unsigned char aLen[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    if( j==0 ) printf("    ");
    printf(" %3d,", aKeywordTable[i].len+aKeywordTable[i].prefix);
    j++;
    if( j>12 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s  };\n", j==0 ? "" : "\n");

  printf("  static const unsigned short int aOffset[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    if( j==0 ) printf("    ");
    printf(" %3d,", aKeywordTable[i].offset);
    j++;
    if( j>12 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s  };\n", j==0 ? "" : "\n");

  printf("  static const unsigned char aCode[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    char *zToken = aKeywordTable[i].zTokenType;
    if( j==0 ) printf("    ");
    printf("%s,%*s", zToken, (int)(14-strlen(zToken)), "");
    j++;
    if( j>=5 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s  };\n", j==0 ? "" : "\n");

  printf("  static const bool aFlag[%d] = {\n", nKeyword);
  for(i=j=0; i<nKeyword; i++){
    bool isReserved = aKeywordTable[i].isReserved;
    const char *flag = (isReserved ? "true" : "false");
    if( j==0 ) printf("    ");
    printf("%s,%*s", flag, (int)(14-strlen(flag)), "");
    j++;
    if( j>=5 ){
      printf("\n");
      j = 0;
    }
  }
  printf("%s  };\n", j==0 ? "" : "\n");

  printf("  int i, j;\n");
  printf("  const char *zKW;\n");
  printf("  if( n>=2 ){\n");
  printf("    i = ((charMap(z[0])*4) ^ (charMap(z[n-1])*3) ^ n) %% %d;\n",
          bestSize);
  printf("    for(i=((int)aHash[i])-1; i>=0; i=((int)aNext[i])-1){\n");
  printf("      if( aLen[i]!=n ) continue;\n");
  printf("      j = 0;\n");
  printf("      zKW = &zText[aOffset[i]];\n");
  printf("      while( j<n && (z[j]&~0x20)==zKW[j] ){ j++; }\n");
  printf("      if( j<n ) continue;\n");
  for(i=0; i<nKeyword; i++){
    printf("      testcase( i==%d ); /* %s */\n",
           i, aKeywordTable[i].zOrigName);
  }
  printf("      *pType = aCode[i];\n");
  printf("      if (pFlag) {\n");
  printf("        *pFlag = aFlag[i];\n");
  printf("      }\n");
  printf("      break;\n");
  printf("    }\n");
  printf("  }\n");
  printf("  return n;\n");
  printf("}\n");
  printf("#define SQL_N_KEYWORD %d\n", nKeyword);
  return 0;
}
