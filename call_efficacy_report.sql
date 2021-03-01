with 
    callresults as (
      select distinct 
        cc.contactscontactid as contactkey
      , t.turf
      , cc.canvassedby as callerid
      , u.firstname as callerfname
      , u.lastname as callerlname
      , cc.datecanvassed
      , cc.vanid as contactid
      , ea.firstname as contactfname
      , ea.lastname as contactlname
      , r.resultshortname as result
    from van.tsm_nextgen_contactscontacts_mym cc
      left join rising.turf t using(vanid)
      left join van.tsm_nextgen_results r using(resultid)
      left join van.tsm_nextgen_users u
     		on u.userid = cc.canvassedby
      left join van.tsm_nextgen_contacts_mym ea using(vanid)
    where t.state = 'ME'
      and cc.contacttypeid = 1
      and cc.inputtypeid in (10,29)
    order by cc.datecanvassed desc
  )
  , volyes as (
      select distinct 
    	  srid.vanid
    	, srn.surveyresponsename
    	, srid.datecanvassed as volyesdate
    	, datediff(day, cr.datecanvassed, srid.datecanvassed) as vdaydiff
      from van.tsm_nextgen_contactssurveyresponses_mym srid
    	inner join van.tsm_nextgen_surveyresponses srn using(surveyresponseid)
    	inner join callresults cr
    		on srid.vanid = cr.contactid
      where srn.surveyquestionid = '369276'
    	and vdaydiff = 0
    	and cr.result = 'Canvassed'
  )
, pledges as (
    select distinct
    	  p.vanid
    	, p.date_collected_pledge
    	, datediff(day, cr.datecanvassed, p.date_collected_pledge) as pdaydiff
    from reporting.pledges p
    	inner join callresults cr
    		on cr.contactid = p.vanid
    where pdaydiff between 0 and 7
    	and cr.result = 'Canvassed'
  )
, signups as (
  select distinct
      cr.contactkey
    , cr.datecanvassed
    , st.datecreated as signupdate
    , su.eventrolename as shifttype
    , su.datetimeoffsetbegin as eventdate
    , datediff(day, cr.datecanvassed, st.datecreated) as sdaydiff
from callresults cr
left join van.tsm_nextgen_eventsignups su
	on su.vanid = cr.contactid
left join van.tsm_nextgen_eventsignupsstatuses st 
	on st.eventsignupid = su.eventsignupid
where st.eventstatusname = 'Scheduled'
	and sdaydiff between 0 and 7
	and cr.result = 'Canvassed'
  )
, publicusers as (
  select distinct 
  	   pu.publicuserid
	,  pu.publicusername as actionidcaller
from van.tsm_nextgen_contactscontacts_mym cc
left join van.tsm_nextgen_publicusers pu
	on cc.canvassedby=pu.publicuserid
where cc.canvassedby not in (select userid from van.tsm_nextgen_users)
  )
  
  
  select distinct 
      crs.contactkey
    , crs.turf
    , crs.datecanvassed
    , crs.callerid
    , crs.callerfname
    , crs.callerlname
    , u.actionidcaller
    , crs.contactid
    , crs.contactfname
    , crs.contactlname
    , crs.result
    , vy.surveyresponsename
    , vy.volyesdate
    , vy.vdaydiff
    , sus.shifttype
    , sus.eventdate
    , sus.sdaydiff
    , datediff(day, vy.volyesdate, sus.signupdate) as vysudaydiff
    , pl.date_collected_pledge
    , pl.pdaydiff
  from callresults crs
    left join volyes vy 
      on vy.vanid = crs.contactid
    left join pledges pl 
      on pl.vanid = crs.contactid
    left join signups sus 
      on crs.contactkey = sus.contactkey
    left join publicusers u
      on u.publicuserid = crs.callerid
  where date_part(y, crs.datecanvassed) = 2020
  	and crs.result <> 'Texted'
  order by crs.datecanvassed desc
