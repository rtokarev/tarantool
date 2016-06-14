#!/usr/bin/env ./tcltestrunner.lua

# 2002 May 24
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.
#
# This file implements tests for joins, including outer joins.
#
# $Id: join.test,v 1.27 2009/07/01 16:12:08 danielk1977 Exp $

set testdir [file dirname $argv0]
source $testdir/tester.tcl

do_test join-1.1 {
  execsql {
    CREATE TABLE t1(a primary key,b,c);
    INSERT INTO t1 VALUES(1,2,3);
    INSERT INTO t1 VALUES(2,3,4);
    INSERT INTO t1 VALUES(3,4,5);
    SELECT * FROM t1;
  }  
} {1 2 3 2 3 4 3 4 5}
do_test join-1.2 {
  execsql {
    CREATE TABLE t2(b primary key,c,d);
    INSERT INTO t2 VALUES(1,2,3);
    INSERT INTO t2 VALUES(2,3,4);
    INSERT INTO t2 VALUES(3,4,5);
    SELECT * FROM t2;
  }  
} {1 2 3 2 3 4 3 4 5}

# # A FROM clause of the form:  "<table>, <table> ON <expr>" is not
# # allowed by the SQLite syntax diagram, nor by any other SQL database
# # engine that we are aware of.  Nevertheless, historic versions of
# # SQLite have allowed it.  We need to continue to support it moving
# # forward to prevent breakage of legacy applications.  Though, we will
# # not advertise it as being supported.
# #
# do_execsql_test join-1.2.1 {
#   SELECT t1.rowid, t2.rowid, '|' FROM t1, t2 ON t1.a=t2.b;
# } {1 1 | 2 2 | 3 3 |}

do_test join-1.3 {
  execsql2 {
    SELECT * FROM t1 NATURAL JOIN t2;
  }
} {a 1 b 2 c 3 d 4 a 2 b 3 c 4 d 5}
do_test join-1.3.1 {
  execsql2 {
    SELECT * FROM t2 NATURAL JOIN t1;
  }
} {b 2 c 3 d 4 a 1 b 3 c 4 d 5 a 2}
do_test join-1.3.2 {
  execsql2 {
    SELECT * FROM t2 AS x NATURAL JOIN t1;
  }
} {b 2 c 3 d 4 a 1 b 3 c 4 d 5 a 2}
do_test join-1.3.3 {
  execsql2 {
    SELECT * FROM t2 NATURAL JOIN t1 AS y;
  }
} {b 2 c 3 d 4 a 1 b 3 c 4 d 5 a 2}
do_test join-1.3.4 {
  execsql {
    SELECT b FROM t1 NATURAL JOIN t2;
  }
} {2 3}

# ticket #3522
do_test join-1.3.5 {
  execsql2 {
    SELECT t2.* FROM t2 NATURAL JOIN t1
  }
} {b 2 c 3 d 4 b 3 c 4 d 5}
do_test join-1.3.6 {
  execsql2 {
    SELECT xyzzy.* FROM t2 AS xyzzy NATURAL JOIN t1
  }
} {b 2 c 3 d 4 b 3 c 4 d 5}
do_test join-1.3.7 {
  execsql2 {
    SELECT t1.* FROM t2 NATURAL JOIN t1
  }
} {a 1 b 2 c 3 a 2 b 3 c 4}
do_test join-1.3.8 {
  execsql2 {
    SELECT xyzzy.* FROM t2 NATURAL JOIN t1 AS xyzzy
  }
} {a 1 b 2 c 3 a 2 b 3 c 4}
do_test join-1.3.9 {
  execsql2 {
    SELECT aaa.*, bbb.* FROM t2 AS aaa NATURAL JOIN t1 AS bbb
  }
} {b 2 c 3 d 4 a 1 b 2 c 3 b 3 c 4 d 5 a 2 b 3 c 4}
do_test join-1.3.10 {
  execsql2 {
    SELECT t1.*, t2.* FROM t2 NATURAL JOIN t1
  }
} {a 1 b 2 c 3 b 2 c 3 d 4 a 2 b 3 c 4 b 3 c 4 d 5}


do_test join-1.4.1 {
  execsql2 {
    SELECT * FROM t1 INNER JOIN t2 USING(b,c);
  }
} {a 1 b 2 c 3 d 4 a 2 b 3 c 4 d 5}
do_test join-1.4.2 {
  execsql2 {
    SELECT * FROM t1 AS x INNER JOIN t2 USING(b,c);
  }
} {a 1 b 2 c 3 d 4 a 2 b 3 c 4 d 5}
do_test join-1.4.3 {
  execsql2 {
    SELECT * FROM t1 INNER JOIN t2 AS y USING(b,c);
  }
} {a 1 b 2 c 3 d 4 a 2 b 3 c 4 d 5}
do_test join-1.4.4 {
  execsql2 {
    SELECT * FROM t1 AS x INNER JOIN t2 AS y USING(b,c);
  }
} {a 1 b 2 c 3 d 4 a 2 b 3 c 4 d 5}
do_test join-1.4.5 {
  execsql {
    SELECT b FROM t1 JOIN t2 USING(b);
  }
} {2 3}

# Ticket #3522
do_test join-1.4.6 {
  execsql2 {
    SELECT t1.* FROM t1 JOIN t2 USING(b);
  }
} {a 1 b 2 c 3 a 2 b 3 c 4}
do_test join-1.4.7 {
  execsql2 {
    SELECT t2.* FROM t1 JOIN t2 USING(b);
  }
} {b 2 c 3 d 4 b 3 c 4 d 5}

do_test join-1.5 {
  execsql2 {
    SELECT * FROM t1 INNER JOIN t2 USING(b);
  }
} {a 1 b 2 c 3 c 3 d 4 a 2 b 3 c 4 c 4 d 5}
do_test join-1.6 {
  execsql2 {
    SELECT * FROM t1 INNER JOIN t2 USING(c);
  }
} {a 1 b 2 c 3 b 2 d 4 a 2 b 3 c 4 b 3 d 5}
do_test join-1.7 {
  execsql2 {
    SELECT * FROM t1 INNER JOIN t2 USING(c,b);
  }
} {a 1 b 2 c 3 d 4 a 2 b 3 c 4 d 5}

do_test join-1.8 {
  execsql {
    SELECT * FROM t1 NATURAL CROSS JOIN t2;
  }
} {1 2 3 4 2 3 4 5}
do_test join-1.9 {
  execsql {
    SELECT * FROM t1 CROSS JOIN t2 USING(b,c);
  }
} {1 2 3 4 2 3 4 5}
do_test join-1.10 {
  execsql {
    SELECT * FROM t1 NATURAL INNER JOIN t2;
  }
} {1 2 3 4 2 3 4 5}
do_test join-1.11 {
  execsql {
    SELECT * FROM t1 INNER JOIN t2 USING(b,c);
  }
} {1 2 3 4 2 3 4 5}
do_test join-1.12 {
  execsql {
    SELECT * FROM t1 natural inner join t2;
  }
} {1 2 3 4 2 3 4 5}

ifcapable subquery {
  do_test join-1.13 {
    execsql2 {
      SELECT * FROM t1 NATURAL JOIN 
        (SELECT b as 'c', c as 'd', d as 'e' FROM t2) as t3
    }
  } {a 1 b 2 c 3 d 4 e 5}
  do_test join-1.14 {
    execsql2 {
      SELECT * FROM (SELECT b as 'c', c as 'd', d as 'e' FROM t2) as 'tx'
          NATURAL JOIN t1
    }
  } {c 3 d 4 e 5 a 1 b 2}
}

do_test join-1.15 {
  execsql {
    CREATE TABLE t3(c primary key,d,e);
    INSERT INTO t3 VALUES(2,3,4);
    INSERT INTO t3 VALUES(3,4,5);
    INSERT INTO t3 VALUES(4,5,6);
    SELECT * FROM t3;
  }  
} {2 3 4 3 4 5 4 5 6}
do_test join-1.16 {
  execsql {
    SELECT * FROM t1 natural join t2 natural join t3;
  }
} {1 2 3 4 5 2 3 4 5 6}
do_test join-1.17 {
  execsql2 {
    SELECT * FROM t1 natural join t2 natural join t3;
  }
} {a 1 b 2 c 3 d 4 e 5 a 2 b 3 c 4 d 5 e 6}
do_test join-1.18 {
  execsql {
    CREATE TABLE t4(d primary key,e,f);
    INSERT INTO t4 VALUES(2,3,4);
    INSERT INTO t4 VALUES(3,4,5);
    INSERT INTO t4 VALUES(4,5,6);
    SELECT * FROM t4;
  }  
} {2 3 4 3 4 5 4 5 6}
do_test join-1.19.1 {
  execsql {
    SELECT * FROM t1 natural join t2 natural join t4;
  }
} {1 2 3 4 5 6}
do_test join-1.19.2 {
  execsql2 {
    SELECT * FROM t1 natural join t2 natural join t4;
  }
} {a 1 b 2 c 3 d 4 e 5 f 6}
do_test join-1.20 {
  execsql {
    SELECT * FROM t1 natural join t2 natural join t3 WHERE t1.a=1
  }
} {1 2 3 4 5}

do_test join-2.1 {
  execsql {
    SELECT * FROM t1 NATURAL LEFT JOIN t2;
  }
} {1 2 3 4 2 3 4 5 3 4 5 {}}

# ticket #3522
do_test join-2.1.1 {
  execsql2 {
    SELECT * FROM t1 NATURAL LEFT JOIN t2;
  }
} {a 1 b 2 c 3 d 4 a 2 b 3 c 4 d 5 a 3 b 4 c 5 d {}}
do_test join-2.1.2 {
  execsql2 {
    SELECT t1.* FROM t1 NATURAL LEFT JOIN t2;
  }
} {a 1 b 2 c 3 a 2 b 3 c 4 a 3 b 4 c 5}
do_test join-2.1.3 {
  execsql2 {
    SELECT t2.* FROM t1 NATURAL LEFT JOIN t2;
  }
} {b 2 c 3 d 4 b 3 c 4 d 5 b {} c {} d {}}

do_test join-2.2 {
  execsql {
    SELECT * FROM t2 NATURAL LEFT OUTER JOIN t1;
  }
} {1 2 3 {} 2 3 4 1 3 4 5 2}
do_test join-2.3 {
  catchsql {
    SELECT * FROM t1 NATURAL RIGHT OUTER JOIN t2;
  }
} {1 {RIGHT and FULL OUTER JOINs are not currently supported}}
do_test join-2.4 {
  execsql {
    SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d
  }
} {1 2 3 {} {} {} 2 3 4 {} {} {} 3 4 5 1 2 3}
do_test join-2.5 {
  execsql {
    SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t1.a>1
  }
} {2 3 4 {} {} {} 3 4 5 1 2 3}
do_test join-2.6 {
  execsql {
    SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t2.b IS NULL OR t2.b>1
  }
} {1 2 3 {} {} {} 2 3 4 {} {} {}}

do_test join-3.1 {
  catchsql {
    SELECT * FROM t1 NATURAL JOIN t2 ON t1.a=t2.b;
  }
} {1 {a NATURAL join may not have an ON or USING clause}}
do_test join-3.2 {
  catchsql {
    SELECT * FROM t1 NATURAL JOIN t2 USING(b);
  }
} {1 {a NATURAL join may not have an ON or USING clause}}
do_test join-3.3 {
  catchsql {
    SELECT * FROM t1 JOIN t2 ON t1.a=t2.b USING(b);
  }
} {1 {cannot have both ON and USING clauses in the same join}}
do_test join-3.4.1 {
  catchsql {
    SELECT * FROM t1 JOIN t2 USING(a);
  }
} {1 {cannot join using column a - column not present in both tables}}
do_test join-3.4.2 {
  catchsql {
    SELECT * FROM t1 JOIN t2 USING(d);
  }
} {1 {cannot join using column d - column not present in both tables}}
# do_test join-3.5 {
#   catchsql { SELECT * FROM t1 USING(a) }
# } {1 {a JOIN clause is required before USING}}
# do_test join-3.6 {
#   catchsql {
#     SELECT * FROM t1 JOIN t2 ON t3.a=t2.b;
#   }
# } {1 {no such column: t3.a}}
# do_test join-3.7 {
#   catchsql {
#     SELECT * FROM t1 INNER OUTER JOIN t2;
#   }
# } {1 {unknown or unsupported join type: INNER OUTER}}
# do_test join-3.8 {
#   catchsql {
#     SELECT * FROM t1 INNER OUTER CROSS JOIN t2;
#   }
# } {1 {unknown or unsupported join type: INNER OUTER CROSS}}
# do_test join-3.9 {
#   catchsql {
#     SELECT * FROM t1 OUTER NATURAL INNER JOIN t2;
#   }
# } {1 {unknown or unsupported join type: OUTER NATURAL INNER}}
# do_test join-3.10 {
#   catchsql {
#     SELECT * FROM t1 LEFT BOGUS JOIN t2;
#   }
# } {1 {unknown or unsupported join type: LEFT BOGUS}}
# do_test join-3.11 {
#   catchsql {
#     SELECT * FROM t1 INNER BOGUS CROSS JOIN t2;
#   }
# } {1 {unknown or unsupported join type: INNER BOGUS CROSS}}
# do_test join-3.12 {
#   catchsql {
#     SELECT * FROM t1 NATURAL AWK SED JOIN t2;
#   }
# } {1 {unknown or unsupported join type: NATURAL AWK SED}}

# do_test join-4.1 {
#   execsql {
#     BEGIN;
#     CREATE TABLE t5(a INTEGER PRIMARY KEY);
#     CREATE TABLE t6(a INTEGER);
#     INSERT INTO t6 VALUES(NULL);
#     INSERT INTO t6 VALUES(NULL);
#     INSERT INTO t6 SELECT * FROM t6;
#     INSERT INTO t6 SELECT * FROM t6;
#     INSERT INTO t6 SELECT * FROM t6;
#     INSERT INTO t6 SELECT * FROM t6;
#     INSERT INTO t6 SELECT * FROM t6;
#     INSERT INTO t6 SELECT * FROM t6;
#     COMMIT;
#   }
#   execsql {
#     SELECT * FROM t6 NATURAL JOIN t5;
#   }
# } {}
# do_test join-4.2 {
#   execsql {
#     SELECT * FROM t6, t5 WHERE t6.a<t5.a;
#   }
# } {}
# do_test join-4.3 {
#   execsql {
#     SELECT * FROM t6, t5 WHERE t6.a>t5.a;
#   }
# } {}
# do_test join-4.4 {
#   execsql {
#     UPDATE t6 SET a='xyz';
#     SELECT * FROM t6 NATURAL JOIN t5;
#   }
# } {}
# do_test join-4.6 {
#   execsql {
#     SELECT * FROM t6, t5 WHERE t6.a<t5.a;
#   }
# } {}
# do_test join-4.7 {
#   execsql {
#     SELECT * FROM t6, t5 WHERE t6.a>t5.a;
#   }
# } {}
# do_test join-4.8 {
#   execsql {
#     UPDATE t6 SET a=1;
#     SELECT * FROM t6 NATURAL JOIN t5;
#   }
# } {}
# do_test join-4.9 {
#   execsql {
#     SELECT * FROM t6, t5 WHERE t6.a<t5.a;
#   }
# } {}
# do_test join-4.10 {
#   execsql {
#     SELECT * FROM t6, t5 WHERE t6.a>t5.a;
#   }
# } {}

# do_test join-5.1 {
#   execsql {
#     BEGIN;
#     create table centros (id integer primary key, centro);
#     INSERT INTO centros VALUES(1,'xxx');
#     create table usuarios (id integer primary key, nombre, apellidos,
#     idcentro integer);
#     INSERT INTO usuarios VALUES(1,'a','aa',1);
#     INSERT INTO usuarios VALUES(2,'b','bb',1);
#     INSERT INTO usuarios VALUES(3,'c','cc',NULL);
#     create index idcentro on usuarios (idcentro);
#     END;
#     select usuarios.id, usuarios.nombre, centros.centro from
#     usuarios left outer join centros on usuarios.idcentro = centros.id;
#   }
# } {1 a xxx 2 b xxx 3 c {}}

# # A test for ticket #247.
# #
# do_test join-7.1 {
#   execsql {
#     CREATE TABLE t7 (x, y);
#     INSERT INTO t7 VALUES ("pa1", 1);
#     INSERT INTO t7 VALUES ("pa2", NULL);
#     INSERT INTO t7 VALUES ("pa3", NULL);
#     INSERT INTO t7 VALUES ("pa4", 2);
#     INSERT INTO t7 VALUES ("pa30", 131);
#     INSERT INTO t7 VALUES ("pa31", 130);
#     INSERT INTO t7 VALUES ("pa28", NULL);

#     CREATE TABLE t8 (a integer primary key, b);
#     INSERT INTO t8 VALUES (1, "pa1");
#     INSERT INTO t8 VALUES (2, "pa4");
#     INSERT INTO t8 VALUES (3, NULL);
#     INSERT INTO t8 VALUES (4, NULL);
#     INSERT INTO t8 VALUES (130, "pa31");
#     INSERT INTO t8 VALUES (131, "pa30");

#     SELECT coalesce(t8.a,999) from t7 LEFT JOIN t8 on y=a;
#   }
# } {1 999 999 2 131 130 999}

# # Make sure a left join where the right table is really a view that
# # is itself a join works right.  Ticket #306.
# #
# ifcapable view {
# do_test join-8.1 {
#   execsql {
#     BEGIN;
#     CREATE TABLE t9(a INTEGER PRIMARY KEY, b);
#     INSERT INTO t9 VALUES(1,11);
#     INSERT INTO t9 VALUES(2,22);
#     CREATE TABLE t10(x INTEGER PRIMARY KEY, y);
#     INSERT INTO t10 VALUES(1,2);
#     INSERT INTO t10 VALUES(3,3);    
#     CREATE TABLE t11(p INTEGER PRIMARY KEY, q);
#     INSERT INTO t11 VALUES(2,111);
#     INSERT INTO t11 VALUES(3,333);    
#     CREATE VIEW v10_11 AS SELECT x, q FROM t10, t11 WHERE t10.y=t11.p;
#     COMMIT;
#     SELECT * FROM t9 LEFT JOIN v10_11 ON( a=x );
#   }
# } {1 11 1 111 2 22 {} {}}
# ifcapable subquery {
#   do_test join-8.2 {
#     execsql {
#       SELECT * FROM t9 LEFT JOIN (SELECT x, q FROM t10, t11 WHERE t10.y=t11.p)
#            ON( a=x);
#     }
#   } {1 11 1 111 2 22 {} {}}
# }
# do_test join-8.3 {
#   execsql {
#     SELECT * FROM v10_11 LEFT JOIN t9 ON( a=x );
#   }
# } {1 111 1 11 3 333 {} {}}
# ifcapable subquery {
#   # Constant expressions in a subquery that is the right element of a
#   # LEFT JOIN evaluate to NULL for rows where the LEFT JOIN does not
#   # match.  Ticket #3300
#   do_test join-8.4 {
#     execsql {
#       SELECT * FROM t9 LEFT JOIN (SELECT 44, p, q FROM t11) AS sub1 ON p=a
#     }
#   } {1 11 {} {} {} 2 22 44 2 111}
# }
# } ;# ifcapable view

# # Ticket #350 describes a scenario where LEFT OUTER JOIN does not
# # function correctly if the right table in the join is really
# # subquery.
# #
# # To test the problem, we generate the same LEFT OUTER JOIN in two
# # separate selects but with on using a subquery and the other calling
# # the table directly.  Then connect the two SELECTs using an EXCEPT.
# # Both queries should generate the same results so the answer should
# # be an empty set.
# #
# ifcapable compound {
# do_test join-9.1 {
#   execsql {
#     BEGIN;
#     CREATE TABLE t12(a,b);
#     INSERT INTO t12 VALUES(1,11);
#     INSERT INTO t12 VALUES(2,22);
#     CREATE TABLE t13(b,c);
#     INSERT INTO t13 VALUES(22,222);
#     COMMIT;
#   }
# } {}

# ifcapable subquery {
#   do_test join-9.1.1 {
#     execsql {
#       SELECT * FROM t12 NATURAL LEFT JOIN t13
#       EXCEPT
#       SELECT * FROM t12 NATURAL LEFT JOIN (SELECT * FROM t13 WHERE b>0);
#     }
#   } {}
# }
# ifcapable view {
#   do_test join-9.2 {
#     execsql {
#       CREATE VIEW v13 AS SELECT * FROM t13 WHERE b>0;
#       SELECT * FROM t12 NATURAL LEFT JOIN t13
#         EXCEPT
#         SELECT * FROM t12 NATURAL LEFT JOIN v13;
#     }
#   } {}
# } ;# ifcapable view
# } ;# ifcapable compound

# ifcapable subquery {
#   # Ticket #1697:  Left Join WHERE clause terms that contain an
#   # aggregate subquery.
#   #
#   do_test join-10.1 {
#     execsql {
#       CREATE TABLE t21(a,b,c);
#       CREATE TABLE t22(p,q);
#       CREATE INDEX i22 ON t22(q);
#       SELECT a FROM t21 LEFT JOIN t22 ON b=p WHERE q=
#          (SELECT max(m.q) FROM t22 m JOIN t21 n ON n.b=m.p WHERE n.c=1);
#     }  
#   } {}

#   # Test a LEFT JOIN when the right-hand side of hte join is an empty
#   # sub-query. Seems fine.
#   #
#   do_test join-10.2 {
#     execsql {
#       CREATE TABLE t23(a, b, c);
#       CREATE TABLE t24(a, b, c);
#       INSERT INTO t23 VALUES(1, 2, 3);
#     }
#     execsql {
#       SELECT * FROM t23 LEFT JOIN t24;
#     }
#   } {1 2 3 {} {} {}}
#   do_test join-10.3 {
#     execsql {
#       SELECT * FROM t23 LEFT JOIN (SELECT * FROM t24);
#     }
#   } {1 2 3 {} {} {}}

# } ;# ifcapable subquery

# #-------------------------------------------------------------------------
# # The following tests are to ensure that bug b73fb0bd64 is fixed.
# #
# do_test join-11.1 {
#   drop_all_tables
#   execsql {
#     CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT);
#     CREATE TABLE t2(a INTEGER PRIMARY KEY, b TEXT);
#     INSERT INTO t1 VALUES(1,'abc');
#     INSERT INTO t1 VALUES(2,'def');
#     INSERT INTO t2 VALUES(1,'abc');
#     INSERT INTO t2 VALUES(2,'def');
#     SELECT * FROM t1 NATURAL JOIN t2;
#   }
# } {1 abc 2 def}

# do_test join-11.2 {
#   execsql { SELECT a FROM t1 JOIN t1 USING (a)}
# } {1 2}
# do_test join-11.3 {
#   execsql { SELECT a FROM t1 JOIN t1 AS t2 USING (a)}
# } {1 2}
# do_test join-11.3 {
#   execsql { SELECT * FROM t1 NATURAL JOIN t1 AS t2}
# } {1 abc 2 def}
# do_test join-11.4 {
#   execsql { SELECT * FROM t1 NATURAL JOIN t1 }
# } {1 abc 2 def}

# do_test join-11.5 {
#   drop_all_tables
#   execsql {
#     CREATE TABLE t1(a COLLATE nocase, b);
#     CREATE TABLE t2(a, b);
#     INSERT INTO t1 VALUES('ONE', 1);
#     INSERT INTO t1 VALUES('two', 2);
#     INSERT INTO t2 VALUES('one', 1);
#     INSERT INTO t2 VALUES('two', 2);
#   }
# } {}
# do_test join-11.6 {
#   execsql { SELECT * FROM t1 NATURAL JOIN t2 }
# } {ONE 1 two 2}
# do_test join-11.7 {
#   execsql { SELECT * FROM t2 NATURAL JOIN t1 }
# } {two 2}

# do_test join-11.8 {
#   drop_all_tables
#   execsql {
#     CREATE TABLE t1(a, b TEXT);
#     CREATE TABLE t2(b INTEGER, a);
#     INSERT INTO t1 VALUES('one', '1.0');
#     INSERT INTO t1 VALUES('two', '2');
#     INSERT INTO t2 VALUES(1, 'one');
#     INSERT INTO t2 VALUES(2, 'two');
#   }
# } {}
# do_test join-11.9 {
#   execsql { SELECT * FROM t1 NATURAL JOIN t2 }
# } {one 1.0 two 2}
# do_test join-11.10 {
#   execsql { SELECT * FROM t2 NATURAL JOIN t1 }
# } {1 one 2 two}

# #-------------------------------------------------------------------------
# # Test that at most 64 tables are allowed in a join.
# #
# do_execsql_test join-12.1 {
#   CREATE TABLE t14(x);
#   INSERT INTO t14 VALUES('abcdefghij');
# }

# proc jointest {tn nTbl res} {
#   set sql "SELECT 1 FROM [string repeat t14, [expr $nTbl-1]] t14;"
#   uplevel [list do_catchsql_test $tn $sql $res]
# }

# jointest join-12.2 30 {0 1}
# jointest join-12.3 63 {0 1}
# jointest join-12.4 64 {0 1}
# jointest join-12.5 65 {1 {at most 64 tables in a join}}
# jointest join-12.6 66 {1 {at most 64 tables in a join}}
# jointest join-12.7 127 {1 {at most 64 tables in a join}}
# jointest join-12.8 128 {1 {at most 64 tables in a join}}
# jointest join-12.9 1000 {1 {at most 64 tables in a join}}

# # If SQLite is built with SQLITE_MEMDEBUG, then the huge number of realloc()
# # calls made by the following test cases are too time consuming to run.
# # Without SQLITE_MEMDEBUG, realloc() is fast enough that these are not
# # a problem.
# ifcapable pragma&&compileoption_diags {
#   if {[lsearch [db eval {PRAGMA compile_options}] MEMDEBUG]<0} {
#     jointest join-12.10 65534 {1 {at most 64 tables in a join}}
#     jointest join-12.11 65535 {1 {too many references to "t14": max 65535}}
#     jointest join-12.12 65536 {1 {too many references to "t14": max 65535}}
#     jointest join-12.13 65537 {1 {too many references to "t14": max 65535}}
#   }
# }


# #-------------------------------------------------------------------------
# # Test a problem with reordering tables following a LEFT JOIN.
# #
# do_execsql_test join-13.0 {
#   CREATE TABLE aa(a);
#   CREATE TABLE bb(b);
#   CREATE TABLE cc(c);

#   INSERT INTO aa VALUES(45);
#   INSERT INTO cc VALUES(45);
#   INSERT INTO cc VALUES(45);
# }

# do_execsql_test join-13.1 {
#   SELECT * FROM aa LEFT JOIN bb, cc WHERE cc.c=aa.a;
# } {45 {} 45 45 {} 45}

# # In the following, the order of [cc] and [bb] must not be exchanged, even
# # though this would be helpful if the query used an inner join.
# do_execsql_test join-13.2 {
#   CREATE INDEX ccc ON cc(c);
#   SELECT * FROM aa LEFT JOIN bb, cc WHERE cc.c=aa.a;
# } {45 {} 45 45 {} 45}


finish_test
