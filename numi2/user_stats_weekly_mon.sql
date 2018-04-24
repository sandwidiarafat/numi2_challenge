
-- DROP FUNCTION public.user_stats_weekly_update_mon(timestamp without time zone);

CREATE OR REPLACE FUNCTION public.user_stats_weekly_update_mon(_datetime timestamp without time zone)
  RETURNS void AS
$BODY$
BEGIN

DELETE from user_stats_weekly_mon where first_day_of_week >= first_day_of_week_mon((_datetime::date - interval '7 days')::date);

--alter SEQUENCE user_stats_weekly_mon_id_seq restart with (select max(id) from user_stats_weekly_mon);
--select setval('user_stats_weekly_mon_id_seq', coalesce((select max(id)+1 from user_stats_weekly_mon), 1), false);
PERFORM SETVAL('user_stats_weekly_mon_id_seq', coalesce((select max(id)+1 from user_stats_weekly_mon), 1), false); 
--PERFORM SETVAL('user_stats_weekly_mon_id_seq', (select case when max(id) is null then 0 else max(id) end from user_stats_weekly_mon) + 1);

INSERT INTO 
  user_stats_weekly_mon
(
  user_id,
  plan_start_date,
 -- weeks_on_plan,
 -- adjusted_plan_start_date,
 -- weeks_on_plan_rev,
  first_day_of_week,
  weight_lbs_lost,
  weight_entries,
  water_consumed,
  water_total_entries,
  water_days_entries,
  water_days_met_goal,
  consumed_days_entries,
  consumed_total_items_logged,
  active_days_entries,
  active_total_duration,
  days_logged,
  created_at,
  updated_at
)



select  p.user_id, null as plan_start_date, 
td.a as first_day_of_week,
weight.lbs_lost as weight_lbs_lost, weight.wh_entries as weight_entries, water.water_consumed, 
water.wtr_entries as water_total_entries, water.days_wtr_entries as water_days_entries, water.days_met_goal as water_days_met_goal,
consumed.days_dh_entries as consumed_days_entries, consumed.total_items_logged as consumed_total_items_logged,
active.times_per_week_activities_logged as active_days_entries, active.wkly_duration as active_tot_duration, logging.days_logged,
now() as created_at, now() as updated_at
from 
	(select p.user_id, p.start_date--cast(p.start_date as date)  
	from (
	select p.user_id, null as start_date from  profiles_numi2 p --where status = 'ONBOARDING_COMPLETE'
	--union 
	--select p2.user_id, null as start_date from profiles p2
	) p) p
inner join (select distinct(user_id) from list_users_c18 -- updated 1/1/2018 from list_users to list_users_c18
where deleted_at is null) lu on p.user_id = lu.user_id  -- updated 5/29/2017 for challenge 2
--cross join td_date td
cross join (select a::date from generate_series(first_day_of_week_mon((_datetime::date - interval '7 days')::date),_datetime::date, '7day'::interval) s(a)) td
left join 
/************ weight **************/
(select wh.user_id, wh.lbs_lost, wh.wh_entries, wh.first_day_of_week from (

		 select ww.user_id, cast(sum(COALESCE(prevwt,current) - current) as numeric(10,2)) as lbs_lost, count(*) as wh_entries,  first_day_of_week_mon(ww.assigned_date::date) as first_day_of_week from (  
		 select w.user_id, w.assigned_date, w.current, 
		 LEAD(w.current) OVER (ORDER BY w.user_id, w.assigned_date desc) as prevwt  from weights_numi2 w
		 inner join profiles_numi2 p on w.user_id = p.user_id
		 order by w.assigned_date ) ww 
		 where  ww.assigned_date >= first_day_of_week_mon((_datetime::date - interval '7 days')::date) --and ww.assigned_date < '2017-11-19'--p.start_date
		 group by ww.user_id,first_day_of_week_mon(ww.assigned_date::date), (last_day_of_week_mon(ww.assigned_date::date) + interval '1 day')::date 
		 
		 ) wh
		  ) weight on p.user_id = weight.user_id and td.a = weight.first_day_of_week

left join 
 /****** waters ******/
(select aa.user_id, first_day_of_week_mon(aa.assigned_date::date) as first_day_of_week, sum(water_consumed_day) as water_consumed, sum(wtr_entries) as wtr_entries,  sum(days_wtr_entries) as days_wtr_entries, sum(days_met_goal) as days_met_goal
from (
select ww.user_id, cast(ww.assigned_date as date) , ww._sum as water_consumed_day, ww2._cnt as wtr_entries, case when _sum >= 64 then 1 else 0 end as days_met_goal, case when ww2._cnt > 0 then 1 else 0 end as days_wtr_entries 
from 
	(SELECT w.user_id, w.assigned_date, (SELECT sum(s) FROM UNNEST(ounces) s) as _sum  from waters_numi2 w) ww
	left join (SELECT w.user_id, w.assigned_date, (SELECT count(s) FROM UNNEST(ounces) s) as _cnt  from waters_numi2 w) ww2 on ww.user_id = ww2.user_id and ww.assigned_date = ww2.assigned_date
	where cast(ww.assigned_date as date) >= first_day_of_week_mon((_datetime::date - interval '7 days')::date)
	--and ww.user_id = 344
) aa
	group by aa.user_id, first_day_of_week_mon(aa.assigned_date::date)
) water on p.user_id = water.user_id and td.a = water.first_day_of_week

left join
/*** diet histories ******/
(select aa.user_id, first_day_of_week_mon(aa.assigned_date::date) as first_day_of_week, count(distinct(aa.assigned_date)) as days_dh_entries, sum(items_consumed) as total_items_logged  from (
	select dh.user_id, dh.assigned_date, count(*) as items_consumed
	from diet_histories_numi2 dh
	where dh.assigned_date >= first_day_of_week_mon((_datetime::date - interval '7 days')::date) and deleted_at is null
	group by dh.user_id, dh.assigned_date
	) aa
group by aa.user_id, first_day_of_week_mon(aa.assigned_date::date)  
--order by aa.user_id, first_day_of_week_mon(aa.assigned_date::date) 
)  consumed on p.user_id = consumed.user_id and td.a = consumed.first_day_of_week

left join 
/************** activity ************************/
(select aa.user_id, first_day_of_week_mon(aa.assigned_date::date) as first_day_of_week, count(distinct(assigned_date)) as times_per_week_activities_logged, sum(dly_duration) as wkly_duration  
from (
	select ah.user_id, ah.assigned_date, count(*) as dly_activities_logged, sum(duration) as dly_duration
	from activity_histories_numi2 ah
	where ah.assigned_date >= first_day_of_week_mon((_datetime::date - interval '7 days')::date) 
	group by ah.user_id, ah.assigned_date
	) aa
group by aa.user_id, first_day_of_week_mon(aa.assigned_date::date) 
) active on p.user_id = active.user_id and td.a = active.first_day_of_week
left join 
/**** distinct logging by date ***/
(
select user_id, first_day_of_week_mon(tt.assigned_date::date) as first_day_of_week, count(*) as days_logged from (

	select ah.user_id, ah.assigned_date
	from activity_histories_numi2 ah
	where ah.assigned_date >= first_day_of_week_mon((_datetime::date - interval '7 days')::date)
	group by ah.user_id, ah.assigned_date
		union
	select dh.user_id, dh.assigned_date
	from diet_histories_numi2 dh
	where dh.assigned_date >= first_day_of_week_mon((_datetime::date - interval '7 days')::date) and deleted_at is null  
		union
	select w.user_id, cast(w.assigned_date as date)
	from waters_numi2 w
	where  cast(w.assigned_date as date) >= first_day_of_week_mon((_datetime::date - interval '7 days')::date) 
	group by w.user_id, w.assigned_date
	) tt
group by tt.user_id, first_day_of_week_mon(tt.assigned_date::date) ) logging on p.user_id = logging.user_id and td.a = logging.first_day_of_week

where (weight.user_id is not null or water.user_id is not null or consumed.user_id is not null or active.user_id is not null) 
order by td.a, p.user_id;


insert into challenge_run_log (function_name, created_at, updated_at)
select 'user_stats_weekly_update_mon' as function_name, now() as created_at, now() as updated_at;


END;
$BODY$
  LANGUAGE plpgsql VOLATILE

