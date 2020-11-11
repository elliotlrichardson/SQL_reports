select

-- ThruText Conversations

	count(distinct case when ttc.ttimestamp > date('2019-01-01') then ttc.conversation_id else null end) as cycle_ttconvos
  , count(distinct case when ttc.ttimestamp > date('2020-08-17') then ttc.conversation_id else null end) as fall_ttconvos 
  , count(distinct case when ttc.ttimestamp > date((current_date-(extract(dow from current_date)-1))) then ttc.conversation_id else null end) as week_ttconvos 
  , count(distinct case when ttc.ttimestamp > date(current_date-1) then ttc.conversation_id else null end) as yesterday_ttconvos
  
from erich

  
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


-- MyV VPB Calls 

  , count(distinct case when mv.datecanvassed > date('2019-01-01') then mv.contactscontactid else null end) as cycle_mvvpbcalls
  , count(distinct case when mv.datecanvassed > date('2020-08-17') then mv.contactscontactid else null end) as fall_mvvpbcalls          
  , count(distinct case when mv.datecanvassed > date((current_date-(extract(dow from current_date)-1))) then mv.contactscontactid else null end) as week_mvvpbcalls
  , count(distinct case when mv.datecanvassed > date(current_date-1) then mv.contactscontactid else null end) as yesterday_mvvpbcalls
          
-- MyV VPB Contacts

  , count(distinct case when mv.datecanvassed > date('2019-01-01') and mv.resultid = 14 then mv.contactscontactid else null end) as cycle_myvvpbcalls
  , count(distinct case when mv.datecanvassed > date('2020-08-17') and mv.resultid = 14 then mv.contactscontactid else null end) as fall_myvvpbcalls          
  , count(distinct case when mv.datecanvassed > date((current_date-(extract(dow from current_date)-1))) and mv.resultid = 14 then mv.contactscontactid else null end) as week_myvvpbcalls
  , count(distinct case when mv.datecanvassed > date(current_date-1) and mv.resultid = 14 then mv.contactscontactid else null end) as yesterday_myvvpbcalls

  
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
  
from erichardson.conversations ttc
	full outer join erichardson.texts ttt using(member_code)
	full outer join erichardson.eavpb ev using(member_code)
	full outer join erichardson.eadialer ed using(member_code)
	full outer join erichardson.myvvpb mv using(member_code)
	full outer join erichardson.myvdialer md using(member_code)
