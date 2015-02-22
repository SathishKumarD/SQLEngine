<<<<<<< HEAD
CREATE TABLE R (A int, B int);
CREATE TABLE S (A int, B int);
SELECT SUM(R.A) AS APP FROM R RAM  WHERE R.A > 4 GROUP BY R.B
=======
CREATE TABLE R (A int, B string);
CREATE TABLE S (A int, B string);
SELECT R.A,R.B FROM R RAM ORDER BY R.A ASC,R.B ASC; 
>>>>>>> branch 'master' of cse562@mjolnir.cse.buffalo.edu:team14.git
