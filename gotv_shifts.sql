with statuses as (
  select distinct
    c.vanid
  , ev.eventid
  , ev.eventname
  , ev.createdbycommitteeid as committee
  , left(su.datetimeoffsetbegin,19) as shift_time
  , su.eventsignupid
  , su.eventshiftid
  , st.eventstatusname
  , st.datemodified
  , row_number() over (partition by st.eventsignupid order by st.datecreated desc) as row
  from van.tsm_nextgen_contacts_mym c
  	inner join van.tsm_nextgen_eventsignups su using(vanid)
  	inner join van.tsm_nextgen_eventsignupsstatuses st using(eventsignupid)
  	inner join van.tsm_nextgen_events ev using(eventid)
  
  
  where ev.eventname ilike 'Final5%'
      and su.datesuppressed is null
      and ev.datesuppressed is null
)  

, signups as (
  
  select distinct
    c.vanid
  , ev.eventname
  , su.eventsignupid
  , st.datecreated as datecreated
  , row_number() over (partition by st.eventsignupid order by st.datecreated asc) as row
  from van.tsm_nextgen_contacts_mym c
  	inner join van.tsm_nextgen_eventsignups su using(vanid)
  	inner join van.tsm_nextgen_eventsignupsstatuses st using(eventsignupid)
  	inner join van.tsm_nextgen_events ev using(eventid)
  
  
  where ev.eventname ilike 'Final5%'
      and su.datesuppressed is null
      and ev.datesuppressed is null
       
  )
  
  , shifts as (
    select distinct
    	  st.vanid
    	, st.eventid
    	, st.eventname
    	, st.shift_time
    	, st.eventshiftid
    	, su.datecreated as signupdate
    	, st.eventstatusname as status
    	, st.datemodified as dateupdated
    	, st.committee
    from statuses st
    	left join signups su using(eventsignupid)
    where st.row = 1
    	and su.row = 1
    )


   , event_state as (
    select vanid, eventid, eventshiftid, estate as state from (
      select 
	 case WHEN ev.createdbycommitteeid = 56351 or (eventname like '%NV%' and eventname not like '%NVRD%') 
	    	THEN 'NV'
              WHEN ev.createdbycommitteeid = 62658 or eventname like '%FL%' 
	    	THEN 'FL'
              WHEN ev.createdbycommitteeid = 56350 or eventname like '%IA%' 
	    	THEN 'IA'
              WHEN ev.createdbycommitteeid = 56354 or eventname like '%PA%' 
	    	THEN 'PA'
              WHEN ev.createdbycommitteeid = 60315 or eventname like '%VA%' 
	    	THEN 'VA'
              WHEN ev.createdbycommitteeid = 64624 or eventname like '%AZ%' 
	    	THEN 'AZ'
              WHEN ev.createdbycommitteeid = 56352 or eventname like '%NH%' 
	    	THEN 'NH'
              WHEN ev.createdbycommitteeid = 57273 or eventname like '%NC%' 
	    	THEN 'NC'
              WHEN ev.createdbycommitteeid = 62659 or eventname like '%MI%' 
	    	THEN 'MI'
              WHEN ev.createdbycommitteeid = 76878 or eventname like '%ME%' 
	    	THEN 'ME'
              WHEN ev.createdbycommitteeid = 62660 or eventname like '%WI%' 
	    	THEN 'WI'
              WHEN ev.createdbycommitteeid = 85292 or eventname like '%National NextGen Organizing%' 
	    	THEN 'DIST'
      	      ELSE 'OTHER' END as estate
	, * 
      from van.tsm_nextgen_events ev 
      where date(ev.dateoffsetbegin) >= date('17-Aug-2020')
    ) a 
    inner join van.tsm_nextgen_eventsignups es using(eventid)
    left join rising.turf_sg_mrr sg using(vanid) 
    
  )

  , turf_sg as (
    select distinct 
	vanid
      , state
      , turf
    from (
      select *
        , row_number() over (partition by vanid, state order by day desc) as row
      from rising.turf_sg_days
    )
    where row=1
  )



select distinct
      t.state as eventstate
    , case when sg.state is not null 
    		then right(sg.turf,2) 
	   when sg.state is null and mrr.state is not null 
	   	then 'OOS' 
	   when sg.state is null and mrr.state is null 
	   	then 'Unturfed' 
	   else null end as turf 
          
    , c.firstname
    , c.lastname
    
    , case when sh.eventname ilike 'DryRun1%' 
    		then 'Dry Run 1' 
	   when sh.eventname ilike 'DryRun2%' 
	   	then 'Dry Run 2' 
           when sh.eventname ilike 'Final5%' 
	   	then 'Final Five' 
           else null end as gotv_event
	   
    , case when sh.eventname ilike '%Phone%' 
    		then 'Phone Bank' 
           when sh.eventname ilike '%Text%' 
	   	then 'Text Bank'
           else null end as shift_type
         
    , date(sh.signupdate) as signup_date
    , case when sh.status ilike 'Sched%' 
    		then 'Scheduled' 
	   when sh.status ilike 'Left Msg' 
	   	then 'Scheduled' 
	   else sh.status end as status
	   
    , sh.dateupdated
    , sh.shift_time
    , date(sh.shift_time) as shift_day
    , case when sh.status ilike 'Sched%' or sh.status ilike 'Conf%' 
    		then 'Y' 
	   else null end as shift_open
    , case when sh.status ilike 'Sched%' 
    		then 'Scheduled' 
	   when sh.status ilike 'Conf%' 
	   	then 'Confirmed' 
	   else null end as open_status
    
    , current_timestamp at time zone 'EDT' as export_time
    
from shifts sh
  left join van.tsm_nextgen_contacts_mym c using(vanid)
  left join event_state t on t.vanid = sh.vanid and t.eventid = sh.eventid and t.eventshiftid = sh.eventshiftid
  left join turf_sg sg on sg.vanid = sh.vanid and sg.state = t.state
  left join rising.turf_sg_mrr mrr on mrr.vanid = sh.vanid

order by 
	eventstate asc
      , turf asc
      , gotv_event asc
      , shift_type asc
      , sh.shift_time asc
      , sh.status asc
      , signupdate desc
      , dateupdated desc
