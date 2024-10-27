-- Databricks notebook source
create or replace table sport_events
select
distinct Sport
from default.athlete_events

-- COMMAND ----------

create or replace table Olympichistory
select
ID,
year,
sport,
name,
age
from
athlete_events
order by
Year

-- COMMAND ----------

select
*
from default.olympichistory

-- COMMAND ----------

select
*
from
default.athlete_events



-- COMMAND ----------

select
min (age) as min_age,
max(age) as max_age
from 
athlete_events
where age >5 and age <50

-- COMMAND ----------

select
year,
count(medal)
from default.athlete_events
where 
Team = 'China'
and
Season = 'Summer'
group by 
Year
order by
Year asc



-- COMMAND ----------

select
distinct sport,
count(medal)
from default.athlete_events
where 
Team = 'China'
and 
Year = 2016
and 
Season = 'Summer'
group by
sport,
medal


-- COMMAND ----------

select
*
from default.sport_events

-- COMMAND ----------

select
avg(Height) as start_height_male,
avg(Weight) as start_weight_male,
avg(age) as start_age_male
from default.athlete_events
where Year= 1896
and 
sex = 'M'
and
Height is not null
and
Weight is not null
and
Age is not null
and sex is not null
and Medal is not Null 

-- COMMAND ----------

select
cast (age as int)
from default.athlete_events
where year > 2000 and year < 2016
and Medal = 'Gold'
order by
Year


-- COMMAND ----------

select
avg(gold_medal.age) as average_age
from 
(select
cast (age as int)
from default.athlete_events
where year > 2000 and year < 2016
and Medal = 'Gold'
order by
Year) gold_medal

-- COMMAND ----------

select
avg(participation_Age.age)
from
(select
cast (age as int)
from default.athlete_events
where year > 2000 and year < 2016
order by
Year)participation_age

-- COMMAND ----------

select
distinct year
from default.athlete_events
where
season = 'Summer'
order by 
year desc


-- COMMAND ----------

create or replace table participation_age_2000
select
distinct participation_age.year as year,
avg(participation_age.age) as average_participation_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where year =2000
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year


-- COMMAND ----------

select
*
from default.participation_age_2000

-- COMMAND ----------

select
* from
athlete_events

-- COMMAND ----------

create or replace table medal_age_2004
select
distinct participation_age.year as year,
avg(participation_age.age) as average_medal_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where medal <>'NA'
and year =2004
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year


-- COMMAND ----------

select
*
from default.medal_age_2004

-- COMMAND ----------

create or replace table medal_age_2008
select
distinct participation_age.year as year,
avg(participation_age.age) as average_medal_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where medal <>'NA'
and year =2008
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year

-- COMMAND ----------

select
*
from
medal_age_2008

-- COMMAND ----------

create or replace table medal_age_2000
select
distinct participation_age.year as year,
avg(participation_age.age) as average_medal_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where medal <>'NA'
and year =2000
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year

-- COMMAND ----------

select
*
from default.medal_age_2000

-- COMMAND ----------

create or replace table participation_age_2004
select
distinct participation_age.year as year,
avg(participation_age.age) as average_participation_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where year =2004
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year

-- COMMAND ----------

select
*
from default.participation_age_2004

-- COMMAND ----------

create or replace table participation_age_2008
select
distinct participation_age.year as year,
avg(participation_age.age) as average_participation_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where year =2008
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year

-- COMMAND ----------

select
*
from default.participation_age_2008

-- COMMAND ----------

create or replace table participation_age_2012
select
distinct participation_age.year as year,
avg(participation_age.age) as average_participation_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where year =2012
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year

-- COMMAND ----------

select
*
from
participation_age_2012

-- COMMAND ----------

create or replace table medal_age_2012
select
distinct participation_age.year as year,
avg(participation_age.age) as average_medal_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where medal <>'NA'
and year =2012
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year

-- COMMAND ----------

select
*
from
medal_age_2012

-- COMMAND ----------

create or replace table participation_age_2016
select
distinct participation_age.year as year,
avg(participation_age.age) as average_participation_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where year =2016
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year

-- COMMAND ----------

select
*
from
participation_age_2016

-- COMMAND ----------

create or replace table medal_age_2016
select
distinct participation_age.year as year,
avg(participation_age.age) as average_medal_age
from 
(select
year,
cast (age as int)
from default.athlete_events
where medal <>'NA'
and year =2016
and season = 'Summer'
order by
Year) participation_age
group by
participation_age.year

-- COMMAND ----------

select
*
from
medal_age_2016

-- COMMAND ----------

create or replace table statistics_2000
select
participation_age_2000.year,
participation_age_2000.average_participation_age,
medal_age_2000.average_medal_age
from
participation_age_2000
 join
medal_age_2000
on
participation_age_2000.year = medal_age_2000.year

-- COMMAND ----------

select
*
from
statistics_2000

-- COMMAND ----------

create or replace table statistics_2004
select
participation_age_2004.year,
participation_age_2004.average_participation_age,
medal_age_2004.average_medal_age
from
participation_age_2004
 join
medal_age_2004
on
participation_age_2004.year = medal_age_2004.year

-- COMMAND ----------

select
*
from default.statistics_2004

-- COMMAND ----------

create or replace table statistics_2008
select
participation_age_2008.year,
participation_age_2008.average_participation_age,
medal_age_2008.average_medal_age
from
participation_age_2008
 join
medal_age_2008
on
participation_age_2008.year = medal_age_2008.year

-- COMMAND ----------

select
*
from
statistics_2008

-- COMMAND ----------

create or replace table statistics_2012
select
participation_age_2012.year,
participation_age_2012.average_participation_age,
medal_age_2012.average_medal_age
from
participation_age_2012
 join
medal_age_2012
on
participation_age_2012.year = medal_age_2012.year

-- COMMAND ----------

select
*
from
statistics_2012

-- COMMAND ----------

create or replace table statistics_2016
select
participation_age_2016.year,
participation_age_2016.average_participation_age,
medal_age_2016.average_medal_age
from
participation_age_2016
 join
medal_age_2016
on
participation_age_2016.year = medal_age_2016.year

-- COMMAND ----------

select
*
from
statistics_2016

-- COMMAND ----------


Create or replace table statistics_21st_century
select
*
from default.statistics_2016
union all
select
*
from default.statistics_2012
union all
select
*
from default.statistics_2008
union all
select
*
from default.statistics_2004
union all
select
*
from default.statistics_2000

-- COMMAND ----------

select
*
from default.statistics_21st_century

-- COMMAND ----------

select
corr(rounded_decimal.avg_participation_age,rounded_decimal.avg_medal_age ) as Correlation
from 
(select
year,
round(average_participation_age,2) as avg_participation_age,
round(average_medal_age,2) as avg_medal_age
from default.statistics_21st_century) rounded_decimal

-- COMMAND ----------

select
year,
round(average_participation_age,2) as avg_participation_age,
round(average_medal_age,2) as avg_medal_age
from default.statistics_21st_century

-- COMMAND ----------

select
*
from
default.athlete_events

-- COMMAND ----------

describe
athlete_events

-- COMMAND ----------

select
ID,
Name,
Sex,
Age,
Cast(Year as INT) as year,
Medal
from default.athlete_events


-- COMMAND ----------

describe
athlete_events

-- COMMAND ----------

describe athlete_events

-- COMMAND ----------

select
*
from 
(select
name,
min(year) as first_participation,
--min(case when medal <> 'NA'  then year  End) as first_medal_year,
max(year) as last_participation
from default.athlete_events_new
where medal <> 'NA'
group by
Name)medal_won
where
(medal_won.last_participation-medal_won.first_participation)>0




-- COMMAND ----------


