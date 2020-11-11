-- ****** Contacts tables ****** --

-- ThruText conversations
drop table if exists erichardson.conversations;
create table erichardson.conversations as (

select 
    member_code
  , conversation_id
  , ttimestamp 
  , responsive
from (
        select 
            tt.member_code 
          , tt.conversation_id
          , tt.ttimestamp
          , row_number() over (partition by tt.conversation_id order by tt.ttimestamp desc) as row
  			  , case when tt.conversation_id in (
            
        			select
            			distinct conversation_id
            	from thrutext.messages
            	where message_direction = 'incoming'
            
          ) then 'Y' else 'N' end as responsive  
  
        from thrutext.messages tt
        where tt.import_source ilike 'nextgenme%'
)
where row = 1
);


-- ThruText messages
drop table if exists erichardson.texts;
create table erichardson.texts as (

select 
		  tt.member_code 
  	, tt.conversation_id
		, tt.ttimestamp
  	, tt.message_id
  	, tt.message_direction
from thrutext.messages tt
where tt.import_source ilike 'nextgenme%'
  
);


-- EA VPB Contacts
drop table if exists erichardson.eavpb;
create table erichardson.eavpb as (
      
select 'NG' as member_code
    , cce.contactscontactid
    , cce.vanid
    , cce.resultid
    , cce.datecanvassed
from van.tsm_nextgen_contactscontacts_mym cce
where cce.contacttypeid = 1
		and cce.inputtypeid in (10,29)
	  and cce.committeeid = '76878'
       
);


-- EA Dialer Contacts
drop table if exists erichardson.eadialer;
create table erichardson.eadialer as (
      
select 'NG' as member_code
  	, cce.contactscontactid
    , cce.vanid
    , cce.resultid
    , cce.datecanvassed
from van.tsm_nextgen_contactscontacts_mym cce
left join van.tsm_nextgen_users u
	on u.userid = cce.canvassedby
where u.canvassername ilike '%ThruTalk%'
	and cce.committeeid = '76878'
    
);


-- MyV VPB Contacts
drop table if exists erichardson.myvvpb;
create table erichardson.myvvpb as (
  
select 'NG' as member_code
  	, ccv.contactscontactid
  	, ccv.vanid
  	, ccv.resultid
    , ccv.datecanvassed
from van.tsm_nextgen_contactscontacts_vf ccv
where ccv.statecode = 'ME'
  	and ccv.inputtypeid in (10,29)
  	and ccv.contacttypeid = 1
  
);
  

-- MyV Dialer Contacts   
drop table if exists erichardson.myvdialer;
create table erichardson.myvdialer as (
      
select 'NG' as member_code
		, ccv.contactscontactid
    , ccv.vanid
    , ccv.resultid
    , ccv.datecanvassed
from van.tsm_nextgen_contactscontacts_vf ccv
left join van.tsm_nextgen_users u
		on u.userid = ccv.canvassedby
where u.canvassername ilike '%ThruTalk%'
		and ccv.statecode = 'ME'
    
);







-- ****** Counts tables ****** --

drop table if exists erichardson.ttccounts;
create table erichardson.ttccounts as (
  
select
    date(current_date) as today
  
-- ThruText Conversations

  , count(distinct case when ttc.ttimestamp > date('2019-01-01') then ttc.conversation_id else null end) as cycle_totalconvos
  , count(distinct case when ttc.ttimestamp > date('2020-08-17') then ttc.conversation_id else null end) as fall_totalconvos
  , count(distinct case when ttc.ttimestamp > date((current_date-(extract(dow from current_date)-1))) then ttc.conversation_id else null end) as week_totalconvos
  , count(distinct case when ttc.ttimestamp > date(current_date-1) then ttc.conversation_id else null end) as yesterday_totalconvos
  , count(distinct case when ttc.ttimestamp > date('2019-01-01') and responsive = 'Y' then ttc.conversation_id else null end) as cycle_realconvos
  , count(distinct case when ttc.ttimestamp > date('2020-08-17') and responsive = 'Y' then ttc.conversation_id else null end) as fall_realconvos 
  , count(distinct case when ttc.ttimestamp > date((current_date-(extract(dow from current_date)-1))) and responsive = 'Y' then ttc.conversation_id else null end) as week_realconvos 
  , count(distinct case when ttc.ttimestamp > date(current_date-1) and responsive = 'Y' then ttc.conversation_id else null end) as yesterday_realconvos
  
  
from erichardson.conversations ttc
);



drop table if exists erichardson.tttcounts;
create table erichardson.tttcounts as (
  
select
    date(current_date) as today
  
-- Outgoing ThruTexts

  , count(distinct case when ttt.message_direction = 'outgoing' and ttt.ttimestamp > date('2019-01-01') then message_id else null end) as cycle_outgoing
  , count(distinct case when ttt.message_direction = 'outgoing' and ttt.ttimestamp > date('2020-08-17') then message_id else null end) as fall_outgoing
  , count(distinct case when ttt.message_direction = 'outgoing' and ttt.ttimestamp > date((current_date-(extract(dow from current_date)-1))) then message_id else null end) as week_outgoing     
  , count(distinct case when ttt.message_direction = 'outgoing' and ttt.ttimestamp > date(current_date-1) then message_id else null end) as yesterday_outgoing
  
-- Incoming ThruTexts

  , count(distinct case when ttt.message_direction = 'incoming' and ttt.ttimestamp > date('2019-01-01') then message_id else null end) as cycle_incoming
  , count(distinct case when ttt.message_direction = 'incoming' and ttt.ttimestamp > date('2020-08-17') then message_id else null end) as fall_incoming
  , count(distinct case when ttt.message_direction = 'incoming' and ttt.ttimestamp > date((current_date-(extract(dow from current_date)-1))) then message_id else null end) as week_incoming    
  , count(distinct case when ttt.message_direction = 'incoming' and ttt.ttimestamp > date(current_date-1) then message_id else null end) as yesterday_incoming
  
from erichardson.texts ttt
);



drop table if exists erichardson.evcounts;
create table erichardson.evcounts as (
  
select
    date(current_date) as today
  
-- EA VPB Calls 

  , count(distinct case when ev.datecanvassed > date('2019-01-01') then ev.contactscontactid else null end) as cycle_eavpbcalls
  , count(distinct case when ev.datecanvassed > date('2020-08-17') then ev.contactscontactid else null end) as fall_eavpbcalls          
  , count(distinct case when ev.datecanvassed > date((current_date-(extract(dow from current_date)-1))) then ev.contactscontactid else null end) as week_eavpbcalls 
  , count(distinct case when ev.datecanvassed > date(current_date-1) then ev.contactscontactid else null end) as yesterday_eavpbcalls
          
-- EA VPB Contacts

  , count(distinct case when ev.datecanvassed > date('2019-01-01') and ev.resultid = 14 then ev.contactscontactid else null end) as cycle_eavpbcontacts
  , count(distinct case when ev.datecanvassed > date('2020-08-17') and ev.resultid = 14 then ev.contactscontactid else null end) as fall_eavpbcontacts       
  , count(distinct case when ev.datecanvassed > date((current_date-(extract(dow from current_date)-1))) and ev.resultid = 14 then ev.contactscontactid else null end) as week_eavpbcontacts
  , count(distinct case when ev.datecanvassed > date(current_date-1) and ev.resultid = 14 then ev.contactscontactid else null end) as yesterday_eavpbcontacts
  
from erichardson.eavpb ev
  );
  
  
  
  drop table if exists erichardson.edcounts;
create table erichardson.edcounts as (
  
select
    date(current_date) as today 
  
-- EA Dialer Calls

  , count(distinct case when ed.datecanvassed > date('2019-01-01') then ed.contactscontactid else null end) as cycle_eadialercalls
  , count(distinct case when ed.datecanvassed > date('2020-08-17') then ed.contactscontactid else null end) as fall_eadialercalls      
  , count(distinct case when ed.datecanvassed > date((current_date-(extract(dow from current_date)-1))) then ed.contactscontactid else null end) as week_eadialercalls
  , count(distinct case when ed.datecanvassed > date(current_date-1) then ed.contactscontactid else null end) as yesterday_eadialercalls

-- EA Dialer Contacts

  , count(distinct case when ed.datecanvassed > date('2019-01-01') and ed.resultid = 14 then ed.contactscontactid else null end) as cycle_eadialercontacts
  , count(distinct case when ed.datecanvassed > date('2020-08-17') and ed.resultid = 14 then ed.contactscontactid else null end) as fall_eadialercontacts    
  , count(distinct case when ed.datecanvassed > date((current_date-(extract(dow from current_date)-1))) and ed.resultid = 14 then ed.contactscontactid else null end) as week_eadialercontacts
  , count(distinct case when ed.datecanvassed > date(current_date-1) and ed.resultid = 14 then ed.contactscontactid else null end) as yesterday_eadialercontacts

from erichardson.eadialer ed
  );
  
  
  
drop table if exists erichardson.mvcounts;
create table erichardson.mvcounts as (
  
  select  
    date(current_date) as today  
  
-- MyV VPB Calls 

  , count(distinct case when mv.datecanvassed > date('2019-01-01') then mv.contactscontactid else null end) as cycle_myvvpbcalls
  , count(distinct case when mv.datecanvassed > date('2020-08-17') then mv.contactscontactid else null end) as fall_myvvpbcalls          
  , count(distinct case when mv.datecanvassed > date((current_date-(extract(dow from current_date)-1))) then mv.contactscontactid else null end) as week_myvvpbcalls
  , count(distinct case when mv.datecanvassed > date(current_date-1) then mv.contactscontactid else null end) as yesterday_myvvpbcalls
          
-- MyV VPB Contacts

  , count(distinct case when mv.datecanvassed > date('2019-01-01') and mv.resultid = 14 then mv.contactscontactid else null end) as cycle_myvvpbcontacts
  , count(distinct case when mv.datecanvassed > date('2020-08-17') and mv.resultid = 14 then mv.contactscontactid else null end) as fall_myvvpbcontacts          
  , count(distinct case when mv.datecanvassed > date((current_date-(extract(dow from current_date)-1))) and mv.resultid = 14 then mv.contactscontactid else null end) as week_myvvpbcontacts 
  , count(distinct case when mv.datecanvassed > date(current_date-1) and mv.resultid = 14 then mv.contactscontactid else null end) as yesterday_myvvpbcontacts 

  from erichardson.myvvpb mv
  );
    
  
  
drop table if exists erichardson.mdcounts;
create table erichardson.mdcounts as (
  
  select  
    date(current_date) as today 
  
-- MyV Dialer Calls

  , count(distinct case when md.datecanvassed > date('2019-01-01') then md.contactscontactid else null end) as cycle_myvdialercalls
  , count(distinct case when md.datecanvassed > date('2020-08-17') then md.contactscontactid else null end) as fall_myvdialercalls     
  , count(distinct case when md.datecanvassed > date((current_date-(extract(dow from current_date)-1))) then md.contactscontactid else null end) as week_myvdialercalls
  , count(distinct case when md.datecanvassed > date(current_date-1) then md.contactscontactid else null end) as yesterday_myvdialercalls

-- MyV Dialer Contacts

  , count(distinct case when md.datecanvassed > date('2019-01-01') and md.resultid = 14 then md.contactscontactid else null end) as cycle_myvdialercontacts
  , count(distinct case when md.datecanvassed > date('2020-08-17') and md.resultid = 14 then md.contactscontactid else null end) as fall_myvdialercontacts    
  , count(distinct case when md.datecanvassed > date((current_date-(extract(dow from current_date)-1))) and md.resultid = 14 then md.contactscontactid else null end) as week_myvdialercontacts
  , count(distinct case when md.datecanvassed > date(current_date-1) and md.resultid = 14 then md.contactscontactid else null end) as yesterday_myvdialercontacts  
  
from erichardson.myvdialer md
  )
