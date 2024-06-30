declare @today_date date;
declare @n int;

set @today_date='2024-06-30';
set @n=3;

select dateadd(week,@n-1,dateadd(day,8-datepart(WEEKDAY,@today_date),@today_date))
