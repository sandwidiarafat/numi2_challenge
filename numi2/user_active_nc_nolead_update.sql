
CREATE OR REPLACE FUNCTION public.user_active_nc_nolead_update(st_date date)
  RETURNS void AS
$BODY$
BEGIN

insert into user_active_nc_nolead(assigned_date, user_id)
select st_date as assigned_date, a.user_id from (
select distinct(dh.user_id) as user_id
from diet_histories dh
where dh.updated_at between st_date and st_date + interval '1 day'
union
select distinct(ah.user_id) as user_id
from activity_histories ah
where  ah.updated_at between st_date and st_date + interval '1 day'
and (external_source is null or external_source not in ('withings','jawbone','striiv','fitbit'))
union
select distinct(w.user_id) as user_id
from waters w
where  w.updated_at between st_date and st_date + interval '1 day'
union
select distinct(wh.user_id) as user_id
from weight_histories wh where wh.created_at between st_date and st_date + interval '1 day'
union
select distinct(p.user_id)  as user_id
from profiles p 
where p.updated_at between st_date and st_date + interval '1 day'
union
select distinct(v.user_id)
from versions v
inner join profiles p on v.user_id = p.user_id
where v.created_at between st_date and st_date + interval '1 day'
) a;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 1000;
ALTER FUNCTION public.user_active_nc_nolead_update(date)
  OWNER TO gfuser;
