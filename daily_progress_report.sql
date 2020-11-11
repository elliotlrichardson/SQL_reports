-- ****** TABLES ****** --


-- Pledges
  drop table if exists erichardson.pledgecounts;
create table erichardson.pledgecounts as (
 with pledges as (
    with ea_forms as (
    select nullif(left(right(REGEXP_SUBSTR(upper(marketsource), 'SOURCE__[A-Z][A-Z]-'), 3), 2), '') as state
      , nullif(split_part(REGEXP_SUBSTR(upper(marketsource), 'SOURCE__[A-Z][A-Z]-[0-Z][0-Z][0-Z]?'), '-', 2), '') as turf
      , vanid
      , of.datecreated
      , date_trunc('day', of.datecreated) as day_signed
    from van.tsm_nextgen_contactsonlineforms_mym of
    left join van.tsm_nextgen_onlineforms_mym o on o.onlineformid = of.onlineformid 
    where state in ('AZ', 'FL', 'IA', 'WI', 'NV', 'NC', 'NH', 'ME', 'PA', 'VA', 'MI') and turf not like '%M%'
  ),

  p2a_forms as (
    select split_part(upper(membership_p2asource), '-', 1) as state
      , split_part(upper(membership_p2asource), '-', 2) as turf
      , l.vanid
      , date(m.date_advocate_joined_campaign_utc) as datecreated 
      , date_trunc('day', date(m.date_advocate_joined_campaign_utc)) as day_signed
    from rising.p2a_memberships m
    left join rising.p2a_log l on m.advocate_id = l.id 
    left join van.tsm_nextgen_contactssurveyresponses_mym csr using(vanid)
    left join van.tsm_nextgen_surveyquestions sq on csr.surveyquestionid = sq.surveyquestionid
    where m.membership_p2asource similar to '[A-Z][A-Z]-[0-Z][0-Z][0-Z]?' 
      and advocate_state in ('AZ', 'FL', 'IA', 'WI', 'NV', 'NC', 'NH', 'ME', 'PA', 'VA', 'MI')
  ),

  all_forms as (
    select * from p2a_forms union select * from ea_forms
  )

select vanid
 			, source_turf
   		, day_canv
      , (current_date-(extract(dow from current_date)-1)) as monday
      , (current_date-(extract(dow from current_date)-1))+1 as tuesday
      , (current_date-(extract(dow from current_date)-1))+2 as wednesday
      , (current_date-(extract(dow from current_date)-1))+3 as thursday
      , (current_date-(extract(dow from current_date)-1))+4 as friday
  		, (current_date-(extract(dow from current_date)-1))+5 as saturday
    from (
      select a.vanid
        , a.geo_state
        , surveyquestionname
        , day_canv
        , date_trunc('week', current_timestamp AT TIME ZONE 'PST'-1) as this_week
        , case when n.notetext like 'Source:%' and of.datecreated < n.datemodified then nullif(left(right(REGEXP_SUBSTR(upper(notetext), 'SOURCE: [A-Z][A-Z]-'), 3), 2), '') else of.state end as source_state
        , case when n.notetext like 'Source:%' and of.datecreated < n.datemodified then nullif(split_part(REGEXP_SUBSTR(upper(notetext), 'SOURCE: [A-Z][A-Z]-[0-Z][0-Z][0-Z]?'), '-', 2), '') else of.turf end as source_turf
        , n.datemodified
        , n.notetext
        , row_number() over (partition by a.vanid order by of.datecreated asc) as row1
      from (
        select sr.vanid
          , c.state as geo_state
          , sq.surveyquestionname
          , sr.datecanvassed
          , date_trunc('week', sr.datecanvassed) as week_canv
          , date_trunc('day', sr.datecanvassed) as day_canv
          , row_number() over (partition by vanid order by datecanvassed asc) as row 
        from van.tsm_nextgen_contactssurveyresponses_mym sr 
        left join van.tsm_nextgen_surveyquestions sq using (surveyquestionid)
        left join van.tsm_nextgen_contacts_mym c using (vanid)
        left join van.tsm_nextgen_contactsnotes_mym n using (vanid)
        where sq.cycle=2020
          and sq.surveyquestionname = 'Pledge 2020'
      ) a 
      
      left join all_forms of on a.vanid = of.vanid and a.day_canv = of.day_signed
      left join van.tsm_nextgen_contactsnotes_mym n on n.vanid = a.vanid
      where row = 1  
    ) b
    where (geo_state in ('AZ', 'FL', 'IA', 'WI', 'NV', 'NC', 'NH', 'ME', 'PA', 'VA', 'MI') or geo_state is null)
    and row1 = 1  
    and source_state = 'ME'
    
    )
  

  select source_turf as turf
        , count(distinct case when p.day_canv = p.monday then p.vanid else null end) as mon_pledges    
		, count(distinct case when p.day_canv = p.tuesday then p.vanid else null end) as tue_pledges 
    , count(distinct case when p.day_canv = p.wednesday then p.vanid else null end) as wed_pledges     
    , count(distinct case when p.day_canv = p.thursday then p.vanid else null end) as thu_pledges             
    , count(distinct case when p.day_canv = p.friday then p.vanid else null end) as fri_pledges             
    , count(distinct case when p.day_canv = p.saturday then p.vanid else null end) as sat_pledges
    from pledges p
    group by turf 
    order by turf
  
  );
  
  -- Active Vols, Vol Leaders 
  drop table if exists erichardson.activecounts;
create table erichardson.activecounts as (
  with active as (
    select vanid
      , state
      , turf
      , vol_leader
      , date_trunc('week', datetimeoff) as shift_week
      , date_trunc('week', time-1) as this_week
      , date_trunc('week', time-1)-7 as last_week
    from (
      --Everyone who completed an active shift, plus vol leader survey response if applicable
      select es.vanid
        , t.state
        , right(t.turf, 2) as turf
        , l.vol_leader
        , date(es.datetimeoffsetbegin) as datetimeoff
        , current_timestamp AT TIME ZONE 'PST' as time
      from van.tsm_nextgen_eventsignups es
      left join van.tsm_nextgen_eventsignupsstatuses st using(eventsignupid)
      left join rising.turf_sg_mrr t using(vanid)
      left join (
        select sr.vanid, resp.surveyresponsename as vol_leader
        from van.tsm_nextgen_contactssurveyresponses_mym sr 
        left join van.tsm_nextgen_surveyquestions sq using (surveyquestionid)
        left join van.tsm_nextgen_surveyresponses resp using(surveyresponseid)
        where sq.cycle=2020
          and sq.surveyquestionname in ('Volunteer Leader')
          and resp.surveyresponsename in ('Confirmed')
      ) l using(vanid)
      left join van.tsm_nextgen_events ev using(eventid)
      where date(es.datetimeoffsetbegin)>=date('2020-08-17')
        and st.eventstatusname='Completed'
        and es.eventrolename in ('Data Entry/ Admin/ Other', 'Volunteer')
        and es.datesuppressed is null
        and ev.datesuppressed is null
	      and t.state ='ME'
        --Limit by phase start date, completed active shifts, and non-deleted shifts/events
    ))
 
    , activevolcounts as (
      select
        turf
      , count(distinct case when shift_week in (this_week, last_week)
              then a.vanid else null end) as week_active_vols
      from active a
      group by turf
      order by turf
      
      )
    
    , volleadercounts as (
      select 
        turf
      , count(distinct case when shift_week in (this_week, last_week) and vol_leader in ('Confirmed')
              then a.vanid else null end) as week_vol_leaders
      from active a
      group by turf
      order by turf
      
      )

  select turf, week_vol_leaders, week_active_vols
  from activevolcounts
  left join volleadercounts using(turf)
   );
 
 -- Completed Shifts
 drop table if exists erichardson.shiftcounts;
create table erichardson.shiftcounts as (
 with compshifts as (
 select 
				vanid
  	  , state
      , turf
      , eventrolename
      , eventstatusname
      , eventid
      , eventshiftid
      , day_canv
      , row
      , (current_date-(extract(dow from current_date)-1)) as monday
      , (current_date-(extract(dow from current_date)-1))+1 as tuesday
      , (current_date-(extract(dow from current_date)-1))+2 as wednesday
      , (current_date-(extract(dow from current_date)-1))+3 as thursday
      , (current_date-(extract(dow from current_date)-1))+4 as friday
  		, (current_date-(extract(dow from current_date)-1))+5 as saturday

 from (
  select vanid
  	  , state
      , turf
      , eventrolename
      , eventstatusname
      , eventid
      , eventshiftid
      , date_trunc('day', datetimeoff) as day_canv
      , row_number() over (partition by vanid || eventid || eventroleid || eventshiftid order by modtime desc, eventstatusname asc) as row
    from (
      select distinct es.vanid
        , es.datetimeoffsetbegin
        , es.eventrolename
        , es.eventroleid
        , st.eventstatusname
        , es.eventid
        , es.eventshiftid
        , t.state
        , t.day
        , st.datecreated
        , case when date(st.datecreated) = date(t.day) then 1 else row_number() over (partition by t.vanid, st.datecreated, es.eventroleid order by t.day asc) end as row1
        , right(t.turf, 2) as turf
        , date(es.datetimeoffsetbegin) as datetimeoff
        , current_timestamp AT TIME ZONE 'PST' as time
        , st.datemodified as modtime
      from van.tsm_nextgen_eventsignups es
      left join van.tsm_nextgen_eventsignupsstatuses st using(eventsignupid)
      left join van.tsm_nextgen_events ev using(eventid)
      left join rising.turf_sg_days t on t.vanid = es.vanid 
      --and t.day = date(es.datetimeoffsetbegin)
      where date(es.datetimeoffsetbegin)>=date('2020-08-17')
        and es.datesuppressed is null
        and ev.datesuppressed is null
        and (date(es.datetimeoffsetbegin) not in (select distinct day from rising.turf_sg_days) or date(es.datetimeoffsetbegin) = t.day)
        --Remove shifts where the event or shift was deleted
    ) a where (date(datecreated) <= date(day))
  ) b
  where row = 1
  and state = 'ME'
  
  )
  

      select 
      turf
    , count(distinct case when eventrolename in ('Data Entry/ Admin/ Other', 'Volunteer') and eventstatusname='Completed' and s.day_canv = s.monday
              then s.vanid||s.eventid||s.eventshiftid  else null end) as mon_shifts          
    , count(distinct case when eventrolename in ('Data Entry/ Admin/ Other', 'Volunteer') and eventstatusname='Completed' and s.day_canv = s.tuesday
              then s.vanid||s.eventid||s.eventshiftid  else null end) as tue_shifts 
    , count(distinct case when eventrolename in ('Data Entry/ Admin/ Other', 'Volunteer') and eventstatusname='Completed' and s.day_canv = s.wednesday
              then s.vanid||s.eventid||s.eventshiftid  else null end) as wed_shifts                
    , count(distinct case when eventrolename in ('Data Entry/ Admin/ Other', 'Volunteer') and eventstatusname='Completed' and s.day_canv = s.thursday
              then s.vanid||s.eventid||s.eventshiftid  else null end) as thu_shifts  
    , count(distinct case when eventrolename in ('Data Entry/ Admin/ Other', 'Volunteer') and eventstatusname='Completed' and s.day_canv = s.friday
              then s.vanid||s.eventid||s.eventshiftid  else null end) as fri_shifts  
    , count(distinct case when eventrolename in ('Data Entry/ Admin/ Other', 'Volunteer') and eventstatusname='Completed' and s.day_canv = s.saturday
              then s.vanid||s.eventid||s.eventshiftid  else null end) as sat_shifts  
    
    from compshifts s
      group by turf
      order by turf
      );
      
    
    
  -- 1:1s - Vol
  
drop table if exists erichardson.oneononecounts;
create table erichardson.oneononecounts as (
 with oneonones as (
  
  select vanid
      , turf
      , surveyresponsename
      , date_trunc('day', datecanvassed) as day_canv
      , (current_date-(extract(dow from current_date)-1)) as monday
      , (current_date-(extract(dow from current_date)-1))+1 as tuesday
      , (current_date-(extract(dow from current_date)-1))+2 as wednesday
      , (current_date-(extract(dow from current_date)-1))+3 as thursday
      , (current_date-(extract(dow from current_date)-1))+4 as friday
  		, (current_date-(extract(dow from current_date)-1))+5 as saturday
    from (
      --Everyone tagged with a 1:1 survey response (any type) since start of phase
      select csr.vanid
        , t.state
        , right(t.turf, 2) as turf
        , sr.surveyresponsename
        , csr.datecanvassed
        , t.day
        , case when date(datecanvassed) = date(day) then 1 else row_number() over (partition by t.vanid, csr.datecanvassed order by t.day asc) end as row
        , current_timestamp AT TIME ZONE 'PST' as time
      from van.tsm_nextgen_contactssurveyresponses_mym csr 
      left join van.tsm_nextgen_surveyquestions sq using (surveyquestionid)
      left join van.tsm_nextgen_surveyresponses sr using(surveyresponseid)
      left join rising.turf_sg_days t on t.vanid = csr.vanid 
      --and t.day = csr.datecanvassed
      where sq.cycle=2020
        and sq.surveyquestionname = 'Completed 1:1'
        and date(csr.datecanvassed)>=date('2020-08-17')
        and (date(csr.datecanvassed) not in (select distinct day from rising.turf_sg_days) or date(csr.datecanvassed) = t.day)
      
        and left(sr.surveyresponsename, 3) = 'Vol'
    ) a where row = 1 and (date(datecanvassed) <= date(day)) and state ='ME'
  
  
  )
  
  
    select turf
    , count(distinct case when o.day_canv = o.monday then o.vanid else null end) as mon_oneonones  
    , count(distinct case when o.day_canv = o.tuesday then o.vanid else null end) as tue_oneonones
    , count(distinct case when o.day_canv = o.wednesday then o.vanid else null end) as wed_oneonones  
    , count(distinct case when o.day_canv = o.thursday then o.vanid else null end) as thu_oneonones  
    , count(distinct case when o.day_canv = o.friday then o.vanid else null end) as fri_oneonones  
    , count(distinct case when o.day_canv = o.saturday then o.vanid else null end) as sat_oneonones
    from oneonones o
    group by turf
    order by turf
  
  );
				 
				 
-- ****** EXPORT ****** --

				 
select p.turf
    , p.mon_pledges
    , p.tue_pledges
    , p.wed_pledges
    , p.thu_pledges
    , p.fri_pledges
    , p.sat_pledges
    , o.mon_oneonones
    , o.tue_oneonones
    , o.wed_oneonones
    , o.thu_oneonones
    , o.fri_oneonones
    , o.sat_oneonones
    , s.mon_shifts
    , s.tue_shifts
    , s.wed_shifts
    , s.thu_shifts
    , s.fri_shifts
    , s.sat_shifts
    , a.week_active_vols
    , a.week_vol_leaders
  from erichardson.pledgecounts p
    full outer join erichardson.activecounts a using(turf)
    full outer join erichardson.shiftcounts s using(turf)
    full outer join erichardson.oneononecounts o using(turf)
  order by turf asc				 
				 
;
