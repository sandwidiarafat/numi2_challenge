-- challenge 2

select count(*) from list_users_bkp
select * from list_users_bkp limit 100

select * from list_users_bkp where list_id = 90

begin;
update list_users_bkp
set list_start_date = '2018-01-01'
where list_id = 90;
commit;


begin;
update list_users_bkp
set list_start_date = '2018-01-08'
where list_id = 91;
commit;


insert into list_users_bkp  --69,627 1/1 --2349 1/2  --2263 1/3  -- 1/4 1606  -- 1/5 1712 --1/6 1574  1/7 1874
(user_id, start_date, list_id, created_at, updated_at, campaign_id, list_start_date)

select t1.user_id, t1.start_date, t1.list_id, t1.created_at, t1.updated_at, t1.campaign_id, t1.list_start_date 
from (
select p.user_id,(p.created_at + tz.utc_offset)::date as start_date, l.id as list_id, now() as created_at, now() as updated_at, l.campaign_id, 	l.list_start_date as list_start_date 
--p.created_at, u.timezone, (p.created_at + tz.utc_offset)::date as offst, tz.utc_offset, now()::date, p.user_id, l.id 
from profiles_numi2 p 
inner join users_numi2 u on p.user_id = u.id
inner join pg_timezone_names tz on tz.name = u.timezone
inner join lists_bkp l on (p.created_at + tz.utc_offset)::date between l.date_start and l.date_end and (p.created_at + tz.utc_offset)::date < now()::date + 1--and l.campaign_id = 3
where p.user_id >= 1500000  and l.campaign_id = 3 --and p.created_at >  now() - interval '7' day
	union
select p.user_id,(dm.profile_start_date + tz.utc_offset)::date as start_date, l.id as list_id, now() as created_at, now() as updated_at, l.campaign_id, l.list_start_date as list_start_date 
--p.created_at, dm.profile_start_date, u.timezone, (dm.profile_start_date + tz.utc_offset)::date as offst, tz.utc_offset, now()::date, p.user_id, l.id 
from profiles_numi2 p 
inner join users_numi2 u on p.user_id = u.id
inner join pg_timezone_names tz on tz.name = u.timezone
inner join data_migrations_numi2 dm on p.user_id = dm.user_id
inner join lists_bkp l on (dm.profile_start_date + tz.utc_offset)::date between l.date_start and l.date_end and (dm.profile_start_date + tz.utc_offset)::date < now()::date + 1 --and l.campaign_id = 3
where p.user_id < 1500000  and l.campaign_id = 3 --and dm.profile_start_date > now() - interval '7' day
) t1
left join list_users_bkp bb on t1.user_id = bb.user_id and t1.campaign_id = bb.campaign_id
 where bb.user_id is null
order by t1.list_id, t1.user_id 


select now()::date + 1
--1.1
insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id, lu.list_id  
from list_users_bkp lu
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp,'2018-01-09'::TIMESTAMP) + 1  between css.day_start and css.day_end and lu.campaign_id = css.campaign_id
where css.rule_type in  (
'date_only',
'date_only_start',
 'date_only_complete'
) and css.active = TRUE and lu.list_start_date <= '2018-01-09'::date + interval '7' day and lu.deleted_at is null and css.campaign_id = 3 -- add lu.deleted_at throughout 1/15/2017
and user_id = 173376
--order by lu.user_id, css.attribute_name
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

select datediff('day', '2018-01-08'::timestamp,'2018-01-02'::TIMESTAMP) + 1
select * from campaign_stream_steps_bkp where id = 107

-- 1.2
--start_date
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, to_char(lu.list_start_date::date, 'MM /DD /YYYY') as data_value, css.id, lu.list_id  
from list_users_bkp lu
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp,'2018-01-08'::TIMESTAMP) + 1 between css.day_start and css.day_end and lu.campaign_id = css.campaign_id
where css.rule_type in  ('start_date') and css.active = TRUE and lu.list_start_date <= '2018-01-08'::date + interval '7' day   and lu.deleted_at is null and css.campaign_id = 3


except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

-- 1.3
--end_date
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, to_char(lu.list_start_date::date + (css.day_in - 1 ) * INTERVAL '1 day', 'MM /DD /YYYY') as data_value, css.id, lu.list_id  
from list_users_bkp lu
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp,'2018-01-08'::TIMESTAMP) + 1 between css.day_start and css.day_end and lu.campaign_id = css.campaign_id
where css.rule_type in  ('end_date') and css.active = TRUE and lu.list_start_date <= '2018-01-08'::date + interval '7' day   and lu.deleted_at is null and css.campaign_id = 3
and list_id = 90

except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events






/** step 2 run tasks count **/

-- task count 1  days loggged food  "C18_C1_TASK_1_COUNT" -- 1/3 2,241  -- 1/4 11,154  -- 1/5 10,673
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
-- 2.1
select usw.user_id, css.attribute_name, css.data_type, cast(sum(usw.consumed_days_entries) as varchar) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from user_stats_weekly_mon_bkp usw
inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-08'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 1 and task_number = 1
where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
having sum(usw.consumed_days_entries) > 0

except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

 select datediff('day', '2018-01-01'::timestamp, '2018-01-08'::timestamp)

	-- 2.1.c2  -- updated 1/7/2018  added for c2 water logging
	select usw.user_id, css.attribute_name, css.data_type, cast(sum(usw.consumed_days_entries) as varchar) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
	from user_stats_weekly_mon_bkp usw
	inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
	inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-09'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 2 and task_number = 1
	where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
	group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
	having sum(usw.water_days_entries) > 0

	except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp


2.2
-- task 1 count set to 0 for any user with no items logged  "C18_C1_TASK_1_COUNT" = 0, check for does not exist
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select tt.user_id, tt.attribute_name, tt.data_type, '0' as data_value, tt.campaign_stream_steps_id, tt.list_users_id from (
select lu.user_id, css.attribute_name, css.data_type,  css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-08'::timestamp) + 1  -- add + 1 at end of date
between css.day_start and css.day_end and css.campaign_id = 3 and rule_type = 'task_count'  and task_number = 1 --and campaign_sequence = 1
where lu.campaign_id = 3
except  select user_id, attribute_name, data_type,  campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp ) tt

 select datediff('day', '2018-01-08'::timestamp, '2018-01-08'::timestamp) + 1

--select distinct(user_id) from appboy_user_events where attribute_name = 'C18_C1_TASK_1_COUNT' order by user_id, data_value
--2.3
-- task count 2  quick logged food "C18_C1_TASK_2_COUNT" bonus task consumbable  --1/6 480  1/7 359
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
--from user_stats_weekly_mon usw
from list_users_bkp lu
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-16'::timestamp)  between css.day_start and css.day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 1 and task_number = 2
inner join 
	(select user_id, assigned_date from diet_histories_numi2 where consumable_type = 'QuickLog' and deleted_at is null group by 1, 2) dh on lu.user_id = dh.user_id and  dh.assigned_date between (lu.list_start_date + interval '1 day' * css.day_start - 	interval '1 day' )::date 
	and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
	where lu.deleted_at is null and lu.campaign_id = 3
--and lu.user_id = 173376
group by lu.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

-- 2.3.c.2  add hit water goal for 1 day  added 1/7/2018
	select usw.user_id, css.attribute_name, css.data_type, cast(sum(usw.consumed_days_entries) as varchar) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
	from user_stats_weekly_mon_bkp usw
	inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
	inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-015'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 2 and task_number = 2
	where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
	group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
	having sum(usw.water_days_met_goal) > 0

	except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp



--2.4
-- task count 2 set for new users as NOT COMPLETED  "C18_C1_TASK_2_COUNT" bonus task consumbable  -- 1/6 0
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, 'NOT COMPLETED' as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-08'::timestamp) + 1  
between css.day_start and css.day_end and css.campaign_id = 3 and rule_type = 'task_count'  and task_number = 2 
--and campaign_sequence = 1
where lu.campaign_id = 3
except  select user_id, attribute_name, data_type, 'NOT COMPLETED' as data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events


/*  STEP 3 */
--select * from appboy_user_events where user_id = 359591 order by date_sent
--select * from campaign_stream_steps css where css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 1
--select * from list_users_c18 limit 100

/* task 1 results validation SUCCESS 'C18_C1_TASK_1_STATUS' set to success */  --1/2 update for 10 --1/6 5413  1/7 2164
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

-- 3.1
select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events where attribute_name = 'C18_C1_TASK_1_COUNT' and cast(data_value as integer) >=5) t1
left join (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu 
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-16'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 1
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp


--3.2
/*results validation SUCCESS 'C18_C1_TASK_2_STATUS' set to success */  --1/2 2236 update  --1/3 594  -- 1/4 762 1/5 480
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events where attribute_name = 'C18_C1_TASK_2_COUNT' and data_value = 'SUCCESS') t1
left join (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_c18 lu 
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-09'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 2
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events


select * from list_users_bkp where list_id = 91


CREATE TABLE public.user_stats_weekly_mon_bkp
(
  id serial,
  user_id integer,
  plan_start_date date,
  weeks_on_plan integer,
  adjusted_plan_start_date date,
  weeks_on_plan_rev integer,
  first_day_of_week date,
  weight_lbs_lost integer,
  weight_entries integer,
  water_consumed integer,
  water_total_entries integer,
  water_days_entries integer,
  water_days_met_goal integer,
  consumed_days_entries integer,
  consumed_total_items_logged integer,
  active_days_entries integer,
  active_total_duration integer,
  created_at timestamp without time zone,
  updated_at timestamp without time zone,
  challenge_in integer,
  list_id integer,
  CONSTRAINT user_stats_weekly_mon_bkp_pkey PRIMARY KEY (id)
);


select * from campaign_stream_steps_bkp where campaign_id = 3 order by id 
select * from campaign_stream_steps where campaign_id = 3 order by id 

begin;
INSERT INTO campaign_stream_steps (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number)
select id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number
from campaign_stream_steps_bkp where id >= 137
order by id;
commit;


INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 137,'C18_C2_ACTIVE_ACTIVE','C18_C2_ACTIVE_ACTIVE',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_ACTIVE','String','ACTIVE',1,1,'D',8,8,'date_only_start',TRUE,1,2,NULL);

INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 138,'C18_C2_ACTIVE_COMPLETE','C18_C2_ACTIVE_COMPLETE',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_ACTIVE','String','COMPLETE',1,1,'D',16,16,'date_only_complete',TRUE,1,10,NULL);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 139,'C18_C2_END_DATE','C18_C2_END_DATE',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_END_DATE','String','DATE',1,14,'D',8,8,'end_date',TRUE,1,3,NULL);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 140,'C18_C2_TASK_1_STATUS_SUCCESS','C18_C2_TASK_1_STATUS_SUCCESS',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_1_STATUS','String','SUCCESS',1,1,'D',8,14,'result_status_success',TRUE,1,6,1);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 141,'C18_C2_TASK_1_STATUS_FAIL','C18_C2_TASK_1_STATUS_FAIL',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_1_STATUS','String','FAIL',1,1,'D',15,15,'result_status_fail',TRUE,1,7,1);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 142,'C18_C2_TASK_1_STATUS_EXPIRE_SUCCESS','C18_C2_TASK_1_STATUS_EXPIRE_SUCCESS',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_1_STATUS','String','EXPIRE_SUCCESS',1,1,'D',22,22,'result_status_success_expiration',TRUE,1,8,1);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 143,'C18_C2_TASK_1_STATUS_EXPIRE_FAIL','C18_C2_TASK_1_STATUS_EXPIRE_FAIL',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_1_STATUS','String','EXPIRE_FAIL',1,1,'D',22,22,'result_status_fail_expiration',FALSE,1,8,1);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 144,'C18_C2_TASK_2_STATUS_SUCCESS','C18_C2_TASK_2_STATUS_SUCCESS',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_2_STATUS','String','SUCCESS',1,1,'D',8,14,'result_status_success',TRUE,1,6,2);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 145,'C18_C2_TASK_2_STATUS_FAIL','C18_C2_TASK_2_STATUS_FAIL',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_2_STATUS','String','FAIL',1,1,'D',15,15,'result_status_fail',TRUE,1,7,2);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 146,'C18_C2_TASK_2_STATUS_EXPIRE_SUCCESS','C18_C2_TASK_2_STATUS_EXPIRE_SUCCESS',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_2_STATUS','String','EXPIRE_SUCCESS',1,1,'D',22,22,'result_status_success_expiration',TRUE,1,8,2);

INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 147,'C18_C2_TASK_2_STATUS_EXPIRE_FAIL','C18_C2_TASK_2_STATUS_EXPIRE_FAIL',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_2_STATUS','String','EXPIRE_FAIL',1,1,'D',22,22,'result_status_fail_expiration',FALSE,1,8,2);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 148,'C18_C2_TASK_1_COUNT','C18_C2_TASK_1_COUNT',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_1_COUNT','Integer','Integer',1,1,'D',8,14,'task_count',TRUE,1,4,1);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 149,'C18_C2_TASK_2_COUNT','C18_C2_TASK_2_COUNT',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_2_COUNT','String','SUCCESS',1,1,'D',8,14,'task_count',TRUE,1,4,2);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 150,'C18_C2_TASK_1_CODE','C18_C2_TASK_1_CODE',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_1_CODE','String','CODE',1,1,'D',8,14,'promo_code',FALSE,1,5,1);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 151,'C18_C2_TASK_2_CODE','C18_C2_TASK_2_CODE',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_2_CODE','String','CODE',1,1,'D',8,14,'promo_code',FALSE,1,5,2);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 152,'C18_C2_TASK_1_CODE_DATE','C18_C2_TASK_1_CODE_DATE',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_1_CODE_DATE','String','DATE',1,1,'D',8,14,'promo_code_end_date',TRUE,1,5,1);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 153,'C18_C2_TASK_2_CODE_DATE','C18_C2_TASK_2_CODE_DATE',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_TASK_2_CODE_DATE','String','DATE',1,1,'D',8,14,'promo_code_end_date',TRUE,1,5,2);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 154,'C18_C2_RESULTS_ALL_TASKS_COMPLETED','C18_C2_RESULTS_ALL_TASKS_COMPLETED',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_RESULTS','String','ALL_TASKS_COMPLETED',1,7,'D',17,17,'result_final',TRUE,1,9,NULL);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 155,'C18_C2_RESULTS_ONLY_TASK1_COMPLETED','C18_C2_RESULTS_ONLY_TASK1_COMPLETED',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_RESULTS','String','ONLY_TASK1_COMPLETED',1,7,'D',17,17,'result_final',TRUE,1,9,NULL);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 156,'C18_C2_RESULTS_ONLY_TASK2_COMPLETED','C18_C2_RESULTS_ONLY_TASK2_COMPLETED',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_RESULTS','String','ONLY_TASK2_COMPLETED',1,7,'D',17,17,'result_final',TRUE,1,9,NULL);
INSERT INTO campaign_stream_steps_bkp (id, step_name, step_description, campaign_id, created_at, updated_at, attribute_name, data_type, data_value, week_in, day_in, frequency, day_start, day_end, rule_type, active, campaign_sequence, campaign_stream_sequence,  task_number) VALUES ( 157,'C18_C2_RESULTS_NONE_COMPLETED','C18_C2_RESULTS_NONE_COMPLETED',3,'2017-12-26 00:00:00.000','2017-12-26 00:00:00.000','C18_C2_RESULTS','String','NONE_COMPLETED',1,7,'D',17,17,'result_final',TRUE,1,9,NULL);
