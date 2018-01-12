
-- steps 00 run user stats weekly
select user_stats_weekly_update_mon('2018-01-11'::timestamp);


select * from list_users_bkp where user_id = 173376

select * from user_stats_weekly_mon where user_id = 173376

select * from appboy_user_events limit 100

select aue.campaign_stream_steps_id, css.attribute_name, list_users_id, count(*) 
from appboy_user_events aue
inner join campaign_stream_steps css on css.id = aue.campaign_stream_steps_id
where css.campaign_id = 4 and date_sent is null
group by 1,2,3
order by 1,2,3

limit 100

-- 0.0 step 0 insert users scripts

insert into list_users_c18  --69,627 1/1 --2349 1/2  --2263 1/3  -- 1/4 1606  -- 1/5 1712 --1/6 1574  1/7 1874
(user_id, start_date, list_id, created_at, updated_at, campaign_id, list_start_date)

select t1.user_id, t1.start_date, t1.list_id, t1.created_at, t1.updated_at, t1.campaign_id, t1.list_start_date 
from (
select p.user_id,(p.created_at + tz.utc_offset)::date as start_date, l.id as list_id, now() as created_at, now() as updated_at, l.campaign_id, 	l.list_start_date as list_start_date 
--p.created_at, u.timezone, (p.created_at + tz.utc_offset)::date as offst, tz.utc_offset, now()::date, p.user_id, l.id 
from profiles_numi2 p 
inner join users_numi2 u on p.user_id = u.id
inner join pg_timezone_names tz on tz.name = u.timezone
inner join lists l on (p.created_at + tz.utc_offset)::date between l.date_start and l.date_end and (p.created_at + tz.utc_offset)::date < now()::date --and l.campaign_id = 3
where p.user_id >= 1500000  and l.campaign_id = 3 --and p.created_at >  now() - interval '7' day
	union
select p.user_id,(dm.profile_start_date + tz.utc_offset)::date as start_date, l.id as list_id, now() as created_at, now() as updated_at, l.campaign_id, l.list_start_date as list_start_date 
--p.created_at, dm.profile_start_date, u.timezone, (dm.profile_start_date + tz.utc_offset)::date as offst, tz.utc_offset, now()::date, p.user_id, l.id 
from profiles_numi2 p 
inner join users_numi2 u on p.user_id = u.id
inner join pg_timezone_names tz on tz.name = u.timezone
inner join data_migrations_numi2 dm on p.user_id = dm.user_id
inner join lists l on (dm.profile_start_date + tz.utc_offset)::date between l.date_start and l.date_end and (dm.profile_start_date + tz.utc_offset)::date < now()::date --and l.campaign_id = 3
where p.user_id < 1500000  and l.campaign_id = 3 --and dm.profile_start_date > now() - interval '7' day
) t1
left join list_users_c18 bb on t1.user_id = bb.user_id and t1.campaign_id = bb.campaign_id
 where bb.user_id is null
order by t1.list_id, t1.user_id 

/** end **/



/** 1.0 step 1 initiate campaigns **/  --1/6 1,574 1/7 1874

-- 1.1
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id, lu.list_id  
from list_users_c18 lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp,'2018-01-11'::TIMESTAMP) + 1 between css.day_start and css.day_end and lu.campaign_id = css.campaign_id
where css.rule_type in  ('date_only','date_only_start', 'date_only_complete') and css.active = TRUE and lu.list_start_date <= '2018-01-11'::date + interval '7' day and lu.deleted_at is null and css.campaign_id = 3 -- add lu.deleted_at throughout 1/15/2017
--order by lu.user_id, css.attribute_name
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events

-- 1.2
--start_date
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, to_char(lu.list_start_date::date, 'MM /DD /YYYY') as data_value, css.id, lu.list_id  
from list_users_c18 lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp,'2018-01-11'::TIMESTAMP) + 1 between css.day_start and css.day_end and lu.campaign_id = css.campaign_id
where css.rule_type in  ('start_date') and css.active = TRUE and lu.list_start_date <= '2018-01-11'::date + interval '7' day   and lu.deleted_at is null and css.campaign_id = 3
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events

-- 1.3
--end_date
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, to_char(lu.list_start_date::date + (css.day_in - 1 ) * INTERVAL '1 day', 'MM /DD /YYYY') as data_value, css.id, lu.list_id  
from list_users_c18 lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp,'2018-01-11'::TIMESTAMP) + 1 between css.day_start and css.day_end and lu.campaign_id = css.campaign_id
where css.rule_type in  ('end_date') and css.active = TRUE and lu.list_start_date <= '2018-01-11'::date + interval '7' day   and lu.deleted_at is null and css.campaign_id = 3
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events

/**  -- changed to user day_in for the end date  1/7/2018
select lu.user_id, css.attribute_name, css.data_type, to_char(lu.list_start_date::date + (css.day_end - 1 ) * INTERVAL '1 day', 'MM /DD /YYYY') as data_value, css.id, lu.list_id  
from list_users_c18 lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp,'2018-01-07'::TIMESTAMP) + 1 between css.day_start and css.day_end and lu.campaign_id = css.campaign_id
where css.rule_type in  ('end_date') and css.active = TRUE and lu.list_start_date <= '2018-01-07'::date + interval '7' day   and lu.deleted_at is null and css.campaign_id = 3
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events
**/ 

select * from campaign_stream_steps where id in (108,139)
select * from campaign_stream_steps_bkp where id in (108,139)

/******************************* step 2 run tasks count **********************************************/

--2.1
-- task count 1  days loggged food  "C18_C1_TASK_1_COUNT" -- 1/3 2,241  -- 1/4 11,154  -- 1/5 10,673
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select usw.user_id, css.attribute_name, css.data_type, cast(sum(usw.consumed_days_entries) as varchar) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from user_stats_weekly_mon usw
inner join list_users_c18 lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 1 and task_number = 1
where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
having sum(usw.consumed_days_entries) > 0
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events

/***************************************************************************************************/
	-- 2.1.c2  C18_C2_TASK_1_COUNT  -- updated 1/7/2018  added for c2 water logging
	insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
	
	select usw.user_id, css.attribute_name, css.data_type, cast(sum(usw.water_days_entries) as varchar) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
	from user_stats_weekly_mon usw
	inner join list_users_c18 lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3
	inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 2 and task_number = 1
	where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
	group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
	having sum(usw.water_days_entries) > 0
	except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events

--select * from user_stats_weekly_mon  where first_day_of_week = '2018-01-08' and water_days_entries > 0  --9921
/***************************************************************************************************/
-- 2.2
-- task 1 count set to 0 for any user with no items logged  "C18_C1_TASK_1_COUNT" = 0, check for does not exist
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select tt.user_id, tt.attribute_name, tt.data_type, '0' as data_value, tt.campaign_stream_steps_id, tt.list_users_id from (
select lu.user_id, css.attribute_name, css.data_type,  css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_c18 lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp) + 1 between css.day_start and css.day_end and css.campaign_id = 3 and rule_type = 'task_count' and task_number = 1
	-- and campaign_sequence = 1 removed campaign sequence 1/7/2018
where lu.campaign_id = 3
except  select user_id, attribute_name, data_type,  campaign_stream_steps_id, list_users_id  from appboy_user_events ) tt


/***************************************************************************************************/
--select distinct(user_id) from appboy_user_events where attribute_name = 'C18_C1_TASK_2_COUNT' order by user_id, data_value
-- 2.3
-- task count 2  quick logged food "C18_C1_TASK_2_COUNT" bonus task consumbable  --1/6 480  1/7 359
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
--from user_stats_weekly_mon usw
from list_users_c18 lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp)  between css.day_start and css.day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 1 and task_number = 2
inner join 
	(select user_id, assigned_date from diet_histories_numi2 where consumable_type = 'QuickLog' and deleted_at is null group by 1, 2) dh on lu.user_id = dh.user_id and  dh.assigned_date between (lu.list_start_date + interval '1 day' * css.day_start - 	interval '1 day' )::date 
	and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
	where lu.deleted_at is null and lu.campaign_id = 3
--and lu.user_id = 173376
group by lu.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events


/***************************************************************************************************/
-- 2.3.c.2  add hit water goal for 1 day  added 1/7/2018
	insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
	
	select usw.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
	from user_stats_weekly_mon usw
	inner join list_users_c18 lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
	inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 2 and task_number = 2
	where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
	group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
	having sum(usw.water_days_met_goal) > 0
	except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events



-- 2.4
-- task count 2 set for new users as NOT COMPLETED  "C18_C1_TASK_2_COUNT" bonus task consumbable  -- 1/6 0
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, css.attribute_name, css.data_type, 'NOT COMPLETED' as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_c18 lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp) + 1 between css.day_start and css.day_end and css.campaign_id = 3 and rule_type = 'task_count' and task_number = 2
	--and campaign_sequence = 1 remove campaign sequence 1/7/2018
except  select user_id, attribute_name, data_type, 'NOT COMPLETED' as data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events


/**************************************  STEP 3 *******************************************/
--select * from appboy_user_events where user_id = 359591 order by date_sent
--select * from campaign_stream_steps css where css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 1
--select * from list_users_c18 limit 100

-- 3.1
/* task 1 results validation SUCCESS 'C18_C1_TASK_1_STATUS' set to success */  --1/2 update for 10 --1/6 5413  1/7 2164

insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events where attribute_name = 'C18_C1_TASK_1_COUNT' and cast(data_value as integer) >=5) t1
--left join
inner join  -- change 1/09
(
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_c18 lu 
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 1
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events



/******************* challenge 2 ******************************** logged water 7 days *************************/
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events where attribute_name = 'C18_C2_TASK_1_COUNT' and cast(data_value as integer) >=7) t1
--left join
inner join  -- change 1/09
(
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_c18 lu 
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 2 and task_number = 1
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events

--select * from appboy_user_events where user_id = 482229  order by created_at and  attribute_name = 'C18_C2_TASK_1_COUNT'

-- 3.2
/*results validation SUCCESS 'C18_C1_TASK_2_STATUS' set to success */  --1/2 2236 update  --1/3 594  -- 1/4 762 1/5 480
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events where attribute_name = 'C18_C1_TASK_2_COUNT' and data_value = 'SUCCESS') t1
--left join 
inner join -- change 1/09
(
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_c18 lu 
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 2
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events

/********************** challenge 2 task 2 ******************************************************************************/
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events where attribute_name like 'C18_C2_TASK_2_COUNT' and data_value = 'SUCCESS') t1
--left join 
inner join -- change 1/09
(
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_c18 lu 
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-11'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and task_number = 2 and campaign_sequence = 2 
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events

--select * from user_stats_weekly_mon where water_days_met_goal > 0 and first_day_of_week = '2018-01-08'

/* * end **/

select * from appboy_user_events where attribute_name = 'C18_C1_TASK_2_STATUS' and data_value = 'SUCCESS' and date_sent is null

begin;
update appboy_user_events
set data_value = 'DO NOT USE'
where attribute_name = 'C18_C1_TASK_2_STATUS' and data_value = 'SUCCESS' and date_sent is null;
commit;


begin;
update appboy_user_events
set date_sent = '2018-01-01'
where attribute_name = 'C18_C1_TASK_2_STATUS' and data_value = 'DO NOT USE' and date_sent is null;
commit;


/** end **/

/** C18_C1_TASK_1_STATUS_FAIL **/
insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select t1.user_id, t1.attribute_name, t1.data_type, t1.data_value, t1.campaign_stream_steps_id, t1.list_users_id
from (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_c18 lu 
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp, '2018-01-08'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_fail' 
and task_number = 1 --and campaign_sequence = 1 
where  lu.campaign_id = 3 ) t1
left join 
(select distinct(user_id) from appboy_user_events where attribute_name like 'C18_C%_TASK_1_STATUS' and data_value = 'SUCCESS') t2 on t1.user_id = t2.user_id
where t2.user_id is  null
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

/** C18_C1_TASK_2_STATUS_FAIL **/
insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
select t1.user_id, t1.attribute_name, t1.data_type, t1.data_value, t1.campaign_stream_steps_id, t1.list_users_id
from (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu 
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-02'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_fail' and campaign_sequence = 1 and task_number = 2
where  lu.campaign_id = 3 ) t1
left join 
(select distinct(user_id) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_2_STATUS' and data_value = 'SUCCESS') t2 on t1.user_id = t2.user_id
where t2.user_id is  null
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp


/*SUCCESS EXPIRATION 'C18_C1_TASK_1_STATUS_EXPIRE_SUCCESS' set to success */

insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_1_STATUS' and data_value = 'SUCCESS') t1
inner join (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu 
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-09'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success_expiration' and campaign_sequence = 1 and task_number = 1
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

/*SUCCESS EXPIRATION 'C18_C1_TASK_2_STATUS_EXPIRE_SUCCESS' set to success */

insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_2_STATUS' and data_value = 'SUCCESS') t1
inner join (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu 
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-09'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success_expiration' and campaign_sequence = 1 and task_number = 2
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

/** results final closeout **/

/*results validation fail 'C18_C1_TASK_2_STATUS' set to success */
insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_2_COUNT' and data_value = 'SUCCESS') t1
left join (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu 
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2017-12-31'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 2
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp



/***beat the streak **/






select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id  
from (select distinct(user_id) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_1_STATUS' and data_value = 'SUCCESS') t1
left join (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu 
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-02'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_fail' and campaign_sequence = 1 and task_number = 1
where  lu.campaign_id = 3 ) t2 on t1.user_id = t2.user_id


select usw.user_id, css.attribute_name, css.data_type, cast(sum(usw.consumed_days_entries) as varchar) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id

select distinct(user_id) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_1_COUNT' and cast(data_value as integer) >=5

select user_id, data_value, count(*) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_1_COUNT' and cast(data_value as integer) >=5
group by 1,2
order by 1,2


select distinct(user_id) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_1_COUNT' and cast(data_value as integer) >=5
and user_id in (
select lu.user_id
from list_users_bkp lu 
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2017-12-31'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 1
where  lu.campaign_id = 3 )

insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
select t1.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.campaign_stream_steps_id, t2.list_users_id 
from (select distinct(user_id) from appboy_user_events_bkp where attribute_name = 'C18_C1_TASK_1_COUNT' and cast(data_value as integer) >=5) t1
left join (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from list_users_bkp lu 
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2017-12-31'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 1
where  lu.campaign_id = 3 )  t2 on t1.user_id = t2.user_id
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
having sum(usw.consumed_days_entries) >=5
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp

select * from campaign_stream_steps_bkp css where css.campaign_id = 3 and css.rule_type = 'result_status_success' and css.campaign_sequence = 1 and css.task_number = 1

-- set results status success
select usw.user_id, css.attribute_name, css.data_type, cast(sum(usw.consumed_days_entries) as varchar) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from user_stats_weekly_mon usw
inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-01'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 1 and task_number = 1
where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
--and lu.user_id = 173376
group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
having sum(usw.consumed_days_entries) >=5
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp


insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select usw.user_id, css.attribute_name, css.data_type, cast(sum(usw.consumed_days_entries) as varchar) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from user_stats_weekly_mon usw
inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2017-12-31'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'result_status_success' and campaign_sequence = 1 and task_number = 1
where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
having sum(usw.consumed_days_entries) >=5
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp



select * from appboy_user_events_bkp ue
where ue.attribute_name = 'C18_C1_TASK_1_COUNT' and cast(ue.data_value as integer) >= 5

and ue.attribute_name



select usw.user_id, css.attribute_name, css.data_type, css.data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id, lu.list_start_date
from user_stats_weekly_mon usw
inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-01'::timestamp)  between day_start and day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 1 and task_number = 1
where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
--and lu.user_id = 173376
group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id, lu.list_start_date
having sum(usw.consumed_days_entries) > 0



select
(lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date as adj_date, (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date as adj_date,
 *, datediff('day', lu.list_start_date::timestamp, '2018-01-01'::timestamp), datediff('day', lu.list_start_date, '2017-12-30'::date)
from user_stats_weekly_mon usw
inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3  and lu.deleted_at is null
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp, '2018-01-01'::timestamp)  between css.day_start and css.day_end and css.campaign_id = 3 and rule_type = 'task_count' and campaign_sequence = 1 and task_number = 1
--where usw.first_day_of_week between lu.list_start_date and lu.list_start_date + interval '27' day
where usw.first_day_of_week between (lu.list_start_date + interval '1 day' * css.day_start - interval '1 day' )::date and (lu.list_start_date + interval '1 day' * css.day_end  - interval '1 day' )::date 
and lu.user_id = 1501413

select * from user_stats_weekly_mon where user_id = 1501413
select '2017-12-25'::date + interval '1' day
select '2017-12-25'::date + interval '7' day
select datediff('day', '2017-12-25'::date, '2017-12-30'::date) 
select datediff('day', '2017-12-25'::date, '2018-01-02'::date) 


select '2017-12-25'::date + interval  day

update user_stats_weekly_testonly
set challenge_in = ci.challenge_update, list_id = ci.list_update
from (
select dd.user_id, dd.first_day_of_week, dd.week_diff, 
case when dd.week_diff in (1,2) then 1 
when dd.week_diff in (3,4) then 2
when dd.week_diff in (5,6) then 3
when dd.week_diff in (7,8) then 4
when dd.week_diff in (9,10) then 5
when dd.week_diff in (11,12) then 6
when dd.week_diff in (13,14) then 7
else null
end as challenge_update,
dd.list_id as list_update
 from 
(select usw.user_id, usw.first_day_of_week, 
datediff('week',  lu.list_start_date, usw.first_day_of_week) + 1 as week_diff,
lu.list_id
from user_stats_weekly_testonly usw 
inner join list_users lu on usw.user_id = lu.user_id and deleted_at is null
--where usw.user_id = 2579
) dd
where week_diff > 0 ) ci where user_stats_weekly_testonly.user_id = ci.user_id and user_stats_weekly_testonly.first_day_of_week = ci.first_day_of_week;



select * from list_users_bkp

select *
from user_stats_weekly usw
inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.campaign_id = 3 
where usw.first_day_of_week >= '2017-12-01'
limit 100

select first_day_of_week, count(*) from user_stats_weekly
group by 1
order by 1


select * from list_users_bkp where campaign_id = 3 and deleted_at is null

select usw.user_id, css.attribute_name, css.data_type, sum(usw.consumed_days_entries) as data_value, css.id as campaign_stream_steps_id, lu.list_id as list_users_id
from user_stats_weekly usw
inner join list_users_bkp lu on lu.user_id = usw.user_id and lu.deleted_at is null and lu.list_id = 17
inner join campaign_stream_steps_bkp css on datediff('day', '2017-04-09'::timestamp, '2017-05-07'::timestamp)  between day_start and day_end and css.campaign_id = 2 and rule_type = 'task_count_2' and task_number = 3
where usw.first_day_of_week between lu.list_start_date and lu.list_start_date + interval '27' day
group by usw.user_id, css.attribute_name, css.id, css.data_type,lu.list_id
having sum(usw.consumed_days_entries) > 0



select t1.user_id, t1.attribute_name, t1.data_type, t1.data_value, t1.id, t1.list_id from (
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id, lu.list_id  from list_users lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp,'2017-12-29'::TIMESTAMP) + 1 = css.day_start and lu.campaign_id = css.campaign_id
where css.rule_type in  ('date_only_2','date_only_start_2','date_only_complete_2') and css.active = TRUE and lu.list_start_date <= '2017-12-29'::date and lu.deleted_at is null  -- add lu.deleted_at throughout 1/15/2017
--order by lu.user_id, css.attribute_name
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events
union all
/*end date*/
--insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
--select t2.user_id, t2.attribute_name, t2.data_type, t2.data_value, t2.id, t2.list_id from (
select lu.user_id, css.attribute_name, css.data_type, to_char(lu.list_start_date::date + (css.day_start + 26) * INTERVAL '1 day', 'MM /DD /YYYY') as data_value, css.id, lu.list_id  from list_users lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp,'2017-12-29'::TIMESTAMP) + 1 = css.day_start and lu.campaign_id = css.campaign_id
where css.rule_type in  ('end_date_2') and css.active = TRUE and lu.list_start_date <= '2017-12-29'::date  and lu.deleted_at is null
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events 
union all
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id, lu.list_id  from list_users lu
inner join campaign_stream_steps css on datediff('day', lu.list_start_date::timestamp,'2017-12-29'::TIMESTAMP) + 1 = css.day_start and lu.campaign_id = css.campaign_id
inner join users u on u.id = lu.user_id
where css.rule_type in  ('date_only_email_check_unsub_2') and css.active = TRUE and lu.list_start_date <= '2017-12-29'::date and lu.deleted_at is null  -- add lu.deleted_at throughout 1/15/2017
and u.receive_emails = 'true' -- check if subscribed to email
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events
) t1
order by t1.list_id, t1.user_id, t1.id;


/** recon work quicklog **/
select * from 
(select * from appboy_user_events where campaign_stream_steps_id = 113 and data_value = 'SUCCESS') t1
left join (select * from appboy_user_events where campaign_stream_steps_id = 117) t2 on t1.user_id = t2.user_id
where t2.user_id is null
order by t1.user_id

select * from appboy_user_events where campaign_stream_steps_id = 106
select * from appboy_user_events where user_id = 1512915 order by date_sent
select * from user_stats_weekly_mon where user_id = 1512915

select * from diet_histories_numi2 where user_id = 1512915 order by assigned_date, created_at
select * from diet
