


--CREATE OR REPLACE FUNCTION consecutive_logging(IN streak_in integer)
--  RETURNS TABLE(user_id integer) AS
$BODY$
      with diet_consecutive as 
      (
        select user_id, assigned_date from (
	select user_id, assigned_date, count(*) from diet_histories where user_id in (
	select distinct (user_id) from diet_histories where assigned_date = current_date - 1)
	--and assigned_date > CURRENT_DATE - 31
	and assigned_date between CURRENT_DATE - 31 and CURRENT_DATE - 1
	and deleted_at is null
	group by user_id, assigned_date
	order by user_id, assigned_date) ds
      ), 
      consecutive_groups as 
      (
	select user_id, assigned_date, assigned_date::DATE - NOW()::DATE - row_number() over (partition by user_id order by assigned_date) as grouping
	from diet_consecutive
      ),
      all_streak_counts as 
      (
	select user_id, max(assigned_date) as maxdate, min(assigned_date) as mindate, count(*) as streak
	from consecutive_groups
	group by user_id, grouping
      )
      select user_id
      from all_streak_counts where maxdate = current_date-1
      group by user_id--, mindate, maxdate
      having max(streak) = streak_in;
      $BODY$
  LANGUAGE sql VOLATILE
  COST 100
  ROWS 1000;


select current_date


CREATE TABLE consumable_streak_summary
(
  id serial,
  user_id integer,
  streak integer,
  max_streak integer,
  created_at timestamp without time zone default now(),
  updated_at timestamp without time zone default now(),
  CONSTRAINT consumable_streak_summary_pkey PRIMARY KEY (id)
);

CREATE INDEX index_consumable_streak_summary_on_user_id
  ON consumable_streak_summary
  USING btree
  (user_id);


alter table consumable_streak_summary
add CONSTRAINT consumable_streak_summary_unique_user_id UNIQUE (user_id)


alter table consumable_streak_summary
add CONSTRAINT consumable_streak_summary_unique_id UNIQUE (user_id, streak)

alter table  consumable_streak_summary
add column maxdate date;

alter table  consumable_streak_summary
add column mindate date;

select * from consumable_streak_summary

insert into consumable_streak_source (user_id, assigned_date, created_at, updated_at)

select t1.user_id, t1.assigned_date, now() as created_at, now() as updated_at from (
select user_id, assigned_date from diet_histories_numi2 where assigned_date between current_date - 30 and current_date - 15 and deleted_at is null
group by user_id, assigned_date ) t1
left join (select user_id, assigned_date from consumable_streak_source) t2 on t1.user_id = t2.user_id and t1.assigned_date = t2.assigned_date
where t2.user_id is null 
--order by user_id, assigned_date
on conflict (user_id, assigned_date) do nothing;

insert into consumable_streak_source (user_id, assigned_date, created_at, updated_at)
select user_id, assigned_date, now() as created_at, now() as updated_at from diet_histories_numi2 where assigned_date between current_date - 30 and current_date - 15 and deleted_at is null
group by user_id, assigned_date
--order by user_id, assigned_date
on conflict on constraint consumable_streak_source_unique_id do nothing;


select count(*) from consumable_streak_source  --1/2 240,580  --1/3 279,488 1/5 293,942 1/6 306784
select * from consumable_streak_source where user_id = 173376 order by assigned_date

select * from consumable_streak_summary where maxdate = '2018-01-06'  --11,978
select * from consumable_streak_summary where user_id = 173376 


/* step 1 run this do insert into source table*/
insert into consumable_streak_source (user_id, assigned_date, created_at, updated_at)

select user_id, assigned_date, now() as created_at, now() as updated_at from diet_histories_numi2 where assigned_date >= current_date - 30 and deleted_at is null
group by user_id, assigned_date
--order by user_id, assigned_date
on conflict on constraint consumable_streak_source_unique_id do nothing;

select count(*) from consumable_streak_summary  -- 1/2 8,198  1/6 11978
select * from consumable_streak_summary where maxdate = '2018-01-06' order by user_id 
select * from consumable_streak_summary where maxdate = '2018-01-06' and streak = 6

/* step 2 run streak */
insert into consumable_streak_summary (user_id, streak, maxdate, mindate, created_at, updated_at)

select user_id, streak, maxdate, mindate, now() as created_at, now() as updated_at from (
with diet_consecutive as 
      (
        select user_id, assigned_date from (
	select user_id, assigned_date, count(*) from consumable_streak_source  where user_id in (
	select distinct (user_id) from consumable_streak_source  where assigned_date = '2018-01-08'::date - 1)
	and assigned_date >= '2018-01-01'::date -- this stays fixed for start date
	and assigned_date <= '2018-01-08'::date - 1
	--and assigned_date between CURRENT_DATE - 31 and CURRENT_DATE - 1
	group by user_id, assigned_date
	order by user_id, assigned_date) ds
      ), 
      consecutive_groups as 
      (
	select user_id, assigned_date, assigned_date::DATE - '2018-01-08'::DATE - row_number() over (partition by user_id order by assigned_date) as grouping
	from diet_consecutive
      ),
      all_streak_counts as 
      (
	select user_id, max(assigned_date) as maxdate, min(assigned_date) as mindate, count(*) as streak
	from consecutive_groups
	group by user_id, grouping
      )
      select user_id, max(streak) as streak, maxdate, mindate
      from all_streak_counts where maxdate = '2018-01-08'::date - 1
      group by user_id, maxdate, mindate--, mindate, maxdate
      --having max(streak) = streak_in;
      ) tt --where user_id = 173376
ON CONFLICT (user_id) do update
set streak = EXCLUDED.streak ,  maxdate = EXCLUDED.maxdate, mindate = EXCLUDED.mindate, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at;

/** step 3 update max streak **/
update consumable_streak_summary 
set max_streak = streak
where streak > coalesce(max_streak,0)

--select * from consumable_streak_summary where streak > coalesce(max_streak,0)
select * from consumable_streak_summary where user_id in (588941)
select * from consumable_streak_source where user_id in (588941)

select * from consumable_streak_summary  where maxdate = '2018-01-05'  --1/5 12,781
select * from consumable_streak_summary  where maxdate = '2018-01-04'  --1/4 2,361
select * from consumable_streak_summary  where maxdate = '2018-01-03'  --1/3 1,624
select * from consumable_streak_summary  where maxdate = '2018-01-02'  --1/2 1,676
select * from consumable_streak_summary  where maxdate = '2018-01-01'  --1/1 794

select maxdate, count(*) from consumable_streak_summary
group by 1
order by 1 desc

select * from list_users_c18 where campaign_id = 3
select distinct(user_id) from appboy_user_events where campaign_stream_steps_id = 127  -- 79131
select * from consumable_streak_summary -- 19236

select tt.user_id, tt.attribute_name, tt.data_type,  tt.data_value, tt.campaign_stream_steps_id, tt.list_users_id  from (  -- 64592
select distinct on (aue.user_id, aue.campaign_stream_steps_id) aue.user_id, aue.attribute_name, aue.data_type,  aue.data_value, aue.campaign_stream_steps_id, aue.list_users_id from appboy_user_events aue
where aue.campaign_stream_steps_id = 129 --and aue.user_id = 173376
 order by user_id,  campaign_stream_steps_id, date_sent desc ) tt
 where cast(tt.data_value as integer) = 0

select * from list_users_c18 where user_id = 511757
select count(*) from appboy_user_events where campaign_stream_steps_id = 129 -- 128630
select * from appboy_user_events where campaign_stream_steps_id = 129 and user_id = 1522375
select * from consumable_streak_summary where user_id = 56309
select * from consumable_streak_source where user_id = 56309

/** set streak campaign as active **/  -- 1/5 1574
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, 'C18_STREAK_ACTIVE' as attribute_name, 'Sting' as data_type, 'ACTIVE' as data_value, 127 as campaign_stream_steps_id, lu.list_id as list_users_id from list_users_c18 lu 
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events where campaign_stream_steps_id = 127


/** step 1 run streak updates **/
-- set zero seed values or reset values for users that have not logged food yesterday -- 1/3 1,693 1/5 1,265
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, 'C18_STREAK_COUNT' as attribute_name, 'Integer' as data_type, '0' as data_value, 129 as campaign_stream_steps_id, lu.list_id as list_users_id 
from list_users_c18 lu
where lu.user_id not in(
select lu.user_id from list_users_c18 lu  -- 1/6 19,235
inner join consumable_streak_summary st on lu.user_id = st.user_id
where lu.campaign_id = 3 
--and st.maxdate = '2018-01-05'::date - 1
)
and lu.campaign_id = 3
except  select tt.user_id, tt.attribute_name, tt.data_type,  tt.data_value, tt.campaign_stream_steps_id, tt.list_users_id  from (
select distinct on (aue.user_id, aue.campaign_stream_steps_id) aue.user_id, aue.attribute_name, aue.data_type,  aue.data_value, aue.campaign_stream_steps_id, aue.list_users_id from appboy_user_events aue
where aue.campaign_stream_steps_id = 129 --and aue.user_id = 173376
 order by user_id,  campaign_stream_steps_id, date_sent desc ) tt
 where cast(tt.data_value as integer) = 0

--select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events



-- step 2
-- post updated streak candidates  -- 1/5 12,780
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, 'C18_STREAK_COUNT' as attribute_name, 'Integer' as data_type, 
cast(tt.streak as varchar) as data_value,  -- change to this # on 1/02/2018
--'0' as data_value, 
129 as campaign_stream_steps_id, lu.list_id as list_users_id from list_users_c18 lu
inner join (
select lu.user_id, streak from list_users_c18 lu
inner join consumable_streak_summary st on lu.user_id = st.user_id
where lu.campaign_id = 3 and st.maxdate = '2018-01-08'::date - 1)  tt on lu.user_id = tt.user_id 
and lu.campaign_id = 3
--and lu.user_id = 173376
except 
(select tt.user_id, tt.attribute_name, tt.data_type,  tt.data_value, tt.campaign_stream_steps_id, tt.list_users_id  from (
select distinct on (aue.user_id, aue.campaign_stream_steps_id) aue.user_id, aue.attribute_name, aue.data_type,  aue.data_value, aue.campaign_stream_steps_id, aue.list_users_id from appboy_user_events aue
where aue.campaign_stream_steps_id = 129 --and aue.user_id = 173376
 order by user_id,  campaign_stream_steps_id, date_sent desc ) tt
 )

-- step 3
-- post un-streak candidates, users that have missed and need to be reset to zero
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, 'C18_STREAK_COUNT' as attribute_name, 'Integer' as data_type, 
--cast(tt.streak as varchar) as data_value,  -- change to this # on 1/02/2018
'0' as data_value, 
129 as campaign_stream_steps_id, lu.list_id as list_users_id from list_users_c18 lu
inner join (
select lu.user_id, streak from list_users_c18 lu
inner join consumable_streak_summary st on lu.user_id = st.user_id
where lu.campaign_id = 3 and st.maxdate < '2018-01-08'::date - 1)  tt on lu.user_id = tt.user_id 
and lu.campaign_id = 3

except  
(select tt.user_id, tt.attribute_name, tt.data_type,  tt.data_value, tt.campaign_stream_steps_id, tt.list_users_id  from (
select distinct on (aue.user_id, aue.campaign_stream_steps_id) aue.user_id, aue.attribute_name, aue.data_type,  aue.data_value, aue.campaign_stream_steps_id, aue.list_users_id from appboy_user_events aue
where aue.campaign_stream_steps_id = 129 --and aue.user_id = 173376
 order by user_id,  campaign_stream_steps_id, date_sent desc ) tt
 where cast(tt.data_value as integer) = 0);


/* old 1/3
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
select lu.user_id, 'C18_STREAK_COUNT' as attribute_name, 'Integer' as data_type, 
cast(tt.streak as varchar) as data_value,  -- change to this # on 1/02/2018
--'0' as data_value, 
129 as campaign_stream_steps_id, lu.list_id as list_users_id from list_users_c18 lu
inner join (
select lu.user_id, streak from list_users_c18 lu
inner join consumable_streak_summary st on lu.user_id = st.user_id
where lu.campaign_id = 3 and st.maxdate = '2018-01-03'::date - 1)  tt on lu.user_id = tt.user_id 
and lu.campaign_id = 3
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events
*/
-- step 4 run streak longest 
/** warning user this to prefill, but then dont't run again **/
-- step 4.1
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, 'C18_STREAK_LONGEST' as attribute_name, 'Integer' as data_type, '0' as data_value, 136 as campaign_stream_steps_id, lu.list_id as list_users_id from list_users_c18 lu
where lu.user_id not in(
select lu.user_id from list_users_c18 lu
inner join consumable_streak_summary st on lu.user_id = st.user_id
where lu.campaign_id = 3 
--and st.maxdate = '2018-01-05'::date - 1
)
and lu.campaign_id = 3
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events where campaign_stream_steps_id = 136 

-- step 4.2
-- update new max streak #s, do next one instead  1/5 11,961
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, 'C18_STREAK_LONGEST' as attribute_name, 'Integer' as data_type, 
cast(tt.max_streak as varchar) as data_value,  -- change to this # on 1/02/2018
--'0' as data_value, 
136 as campaign_stream_steps_id, lu.list_id as list_users_id from list_users_c18 lu
inner join (
select lu.user_id, max_streak from list_users_c18 lu
inner join consumable_streak_summary st on lu.user_id = st.user_id
where lu.campaign_id = 3 and st.maxdate = '2018-01-08'::date - 1)  tt on lu.user_id = tt.user_id 
and lu.campaign_id = 3 --and lu.user_id = 173376
--except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events
except 
(select tt.user_id, tt.attribute_name, tt.data_type,  tt.data_value, tt.campaign_stream_steps_id, tt.list_users_id  from (
select distinct on (aue.user_id, aue.campaign_stream_steps_id) aue.user_id, aue.attribute_name, aue.data_type,  aue.data_value, aue.campaign_stream_steps_id, aue.list_users_id from appboy_user_events aue
where aue.campaign_stream_steps_id = 136 --and aue.user_id = 173376
 order by user_id,  campaign_stream_steps_id, date_sent desc ) tt
 )

-- step 4.3
-- adjust to update max streak numberse for all users -- final cleanup
insert into appboy_user_events (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)

select lu.user_id, 'C18_STREAK_LONGEST' as attribute_name, 'Integer' as data_type, 
cast(tt.max_streak as varchar) as data_value,  -- change to this # on 1/02/2018
--'0' as data_value, 
136 as campaign_stream_steps_id, lu.list_id as list_users_id from list_users_c18 lu
inner join (
select lu.user_id, max_streak from list_users_c18 lu
inner join consumable_streak_summary st on lu.user_id = st.user_id
where lu.campaign_id = 3 
--and st.maxdate < '2018-01-04'::date - 1
)  tt on lu.user_id = tt.user_id 
and lu.campaign_id = 3
except (select tt.user_id, tt.attribute_name, tt.data_type,  tt.data_value, tt.campaign_stream_steps_id, tt.list_users_id  from (
select distinct on (aue.user_id, aue.campaign_stream_steps_id) aue.user_id, aue.attribute_name, aue.data_type,  aue.data_value, aue.campaign_stream_steps_id, aue.list_users_id from appboy_user_events aue
where aue.campaign_stream_steps_id = 136 --and aue.user_id = 173376
 order by user_id,  campaign_stream_steps_id, date_sent desc ) tt
 )

/** stop **/

select * from appboy_user_events where user_id = 1524107
select * from consumable_streak_summary where  user_id = 1524107

select tt.user_id, tt.attribute_name, tt.data_type,  tt.data_value, tt.campaign_stream_steps_id, tt.list_users_id  from (
select distinct on (aue.user_id, aue.campaign_stream_steps_id) aue.user_id, aue.attribute_name, aue.data_type,  aue.data_value, aue.campaign_stream_steps_id, aue.list_users_id from appboy_user_events aue
where aue.campaign_stream_steps_id = 136 and aue.user_id = 482171
 order by user_id,  campaign_stream_steps_id, date_sent desc ) tt




select * from consumable_streak_summary where user_id = 290388
select * from appboy_user_events where user_id = 1517835 and campaign_stream_steps_id = 136 order by date_sent desc

select * from consumable_streak_summary where user_id = 482171
select * from appboy_user_events where user_id = 482171 and campaign_stream_steps_id = 136 order by date_sent desc

select campaign_stream_steps_id, count(*) from appboy_user_events where date_sent is null
group by 1

select campaign_stream_steps_id,  * from appboy_user_events where date_sent is null
order by  user_id,  campaign_stream_steps_id

insert into appboy_user_events_bkp (user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id)
select lu.user_id, css.attribute_name, css.data_type, css.data_value, css.id, lu.list_id  
from list_users_bkp lu
inner join campaign_stream_steps_bkp css on datediff('day', lu.list_start_date::timestamp,'2018-01-01'::TIMESTAMP) + 1 between css.day_start and css.day_end and lu.campaign_id = css.campaign_id
where css.rule_type in  ('date_only','date_only_start', 'date_only_complete') and css.active = TRUE and lu.list_start_date <= '2018-01-01'::date + interval '7' day and lu.deleted_at is null and css.campaign_id = 3 -- add lu.deleted_at throughout 1/15/2017
--order by lu.user_id, css.attribute_name
except  select user_id, attribute_name, data_type, data_value, campaign_stream_steps_id, list_users_id  from appboy_user_events_bkp


   select user_id, assigned_date from (
	select user_id, assigned_date from consumable_streak_source where user_id in (
	select distinct (user_id) from consumable_streak_source where assigned_date = current_date - 1)
	--and assigned_date > CURRENT_DATE - 31
	and assigned_date between CURRENT_DATE - 31 and CURRENT_DATE - 1
	order by user_id, assigned_date) ds


select current_date
select now()
select '2017-12-05'::date - now()::date

select * from consumable_streak_source where assigned_date = '2018-01-01'

select * from consumable_streak_summary

/* run streak */
insert into consumable_streak_summary (user_id, streak, maxdate, mindate, created_at, updated_at)

select user_id, streak, maxdate, mindate, now() as created_at, now() as updated_at from (
with diet_consecutive as 
      (
        select user_id, assigned_date from (
	select user_id, assigned_date, count(*) from consumable_streak_source  where user_id in (
	select distinct (user_id) from consumable_streak_source  where assigned_date = '2018-01-02'::date - 1)
	and assigned_date >= '2018-01-01'::date -- this stays fixed for start date
	and assigned_date <= '2018-01-02'::date - 1
	--and assigned_date between CURRENT_DATE - 31 and CURRENT_DATE - 1
	group by user_id, assigned_date
	order by user_id, assigned_date) ds
      ), 
      consecutive_groups as 
      (
	select user_id, assigned_date, assigned_date::DATE - '2018-01-02'::DATE - row_number() over (partition by user_id order by assigned_date) as grouping
	from diet_consecutive
      ),
      all_streak_counts as 
      (
	select user_id, max(assigned_date) as maxdate, min(assigned_date) as mindate, count(*) as streak
	from consecutive_groups
	group by user_id, grouping
      )
      select user_id, max(streak) as streak, maxdate, mindate
      from all_streak_counts where maxdate = '2018-01-02'::date - 1
      group by user_id, maxdate, mindate--, mindate, maxdate
      --having max(streak) = streak_in;
      ) tt
on conflict (user_id) do update
set streak = EXCLUDED.streak ,  maxdate = EXCLUDED.maxdate, mindate = EXCLUDED.mindate, 
created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at;

select * from consumable_streak_summary 

  select * from consumable_streak_source where user_id = 173376 order by assigned_date desc

select *  from consumable_streak_source order by created_at desc limit 100

     select user_id, assigned_date from (
	select user_id, assigned_date, count(*) from diet_histories_numi2 where user_id in (
	select distinct (user_id) from diet_histories_numi2 where assigned_date = current_date - 1 and deleted_at is null)
	--and assigned_date > CURRENT_DATE - 31
	and assigned_date between CURRENT_DATE - 31 and CURRENT_DATE - 1
	and deleted_at is null
	group by user_id, assigned_date
	order by user_id, assigned_date) ds