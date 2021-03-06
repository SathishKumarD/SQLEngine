CREATE TABLE PLAYERS(
  ID string, 
  FIRSTNAME string, 
  LASTNAME string, 
  FIRSTSEASON int, 
  LASTSEASON int, 
  WEIGHT int, 
  BIRTHDATE date
);

SELECT FIRSTSEASON, 
SUM(WEIGHT) ,
SUM(WEIGHT*2) ,
AVG(WEIGHT),
AVG(WEIGHT*2),
AVG(WEIGHT*3),
SUM(WEIGHT*3),
COUNT(WEIGHT)
FROM PLAYERS 
WHERE FIRSTSEASON = 2007
GROUP BY FIRSTSEASON
