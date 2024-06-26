with cte as (
  select Team_1 as team,
         (case when Team_1=Winner then 1 else 0 end) as win
  from icc_world_cup
  union all
  select Team_2 as team,
         (case when Team_2=Winner then 1 else 0 end) as win
  from icc_world_cup
)
select team as Team_name ,count(1) as Total_Matches, sum(win)as Total_Wins, (count(1)-sum(win))as Total_Losses 
from cte 
group by team 
order by Total_Wins desc
