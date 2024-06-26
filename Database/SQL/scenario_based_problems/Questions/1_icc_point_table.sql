/*
You have the ICC tournament winners' details. Please find the team,  No of matches played, winning count, and losses.

input:
======
Team-1 Team_2  winner
India	  SL	   India
SL	    Aus	   Aus
SA	    Eng	   Eng
Eng	    NZ	   NZ
Aus	   India	 India

Expected output:
================
Team, Total_Matches,Total_Wins,Total_Losses 
India	2	2	0
NZ	1	1	0
Aus	2	1	1
Eng	2	1	1
SA	1	0	1
SL	2	0	2

*/

--Create table & insert data
create table icc_world_cup(
Team_1 Varchar(20),
Team_2 Varchar(20),
Winner Varchar(20)
);

INSERT INTO icc_world_cup values('India','SL','India');
INSERT INTO icc_world_cup values('SL','Aus','Aus');
INSERT INTO icc_world_cup values('SA','Eng','Eng');
INSERT INTO icc_world_cup values('Eng','NZ','NZ');
INSERT INTO icc_world_cup values('Aus','India','India');

