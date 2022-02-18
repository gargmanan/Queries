with a as (SELECT
    distinct sdh.msg.case_uuid as case_uuid,

    sdh.msg.queue_name as queue_name,--- Queue Name as on sherlock
    sdh.msg.queue_uuid as queue_uuid, ----queue ID 
    sdh.msg.entity_uuid as driver_uuid_1,---- Driver Uuid
    sdh.msg.vote_final_decision as action_reference,
   -- sdh.msg.vote_details as comments,
    SPLIT_PART(SPLIT_PART(SPLIT_PART(SPLIT_PART(sdh.msg.case_properties,'"driver_uuid_2":',2),',',1),'}',1),'"',2) as driver_uuid_2,
    SPLIT_PART(SPLIT_PART(SPLIT_PART(SPLIT_PART(sdh.msg.case_properties,'"uuid_1_ranked_score":',2),',',1),'}',1),'"',2) as uuid_1_ranked_score,
    SPLIT_PART(SPLIT_PART(SPLIT_PART(SPLIT_PART(sdh.msg.case_properties,'"uuid_2_ranked_score":',2),',',1),'}',1),'"',2) as uuid_2_ranked_score,

    
   -- COALESCE(REGEXP_SUBSTR(sdh.msg.case_properties,'"name":"(\d+\w{0,1})"',1,1,'',1),
           --  REGEXP_SUBSTR(sdh.msg.case_properties,'"name":"(\w{1}\d+)"',1,1,'',1),
           --  REGEXP_SUBSTR(sdh.msg.case_properties,'"name":"([^"]+)"',1,1,'',1)
          --   ) as rule_name,
             
    CAST(SUBSTRING(sdh.msg.created_at,1,10) as DATE) as created_at,
    date_trunc ('week', from_iso8601_date(sdh.datestr)) as weekofyear,
   -- sbph.msg.agent_uuid as agent_uuid,
    CONCAT(ddc.firstname, ' ', ddc.lastname) as agent_name
    
 from rawdata.kafka_hp_sherlock_decision_history_nodedup sdh --- Sherlock DB 
    left join  rawdata.kafka_hp_sherlock_button_pressed_history_nodedup sbph ----Sherlock Decision DB
  on sdh.msg.case_uuid = sbph.msg.case_uuid
    left join dwh.dim_client ddc ---- Driver DB 
  on sbph.msg.agent_uuid = ddc.user_uuid

and sdh.datestr >= '2021-05-20' 
and sbph.datestr >= '2021-05-20' 
and sdh.msg.queue_uuid = '5ae77f29-0bda-4a15-94c0-2f309085fb88' ------ Edit the Sherlock Queue Here 
and  sbph.msg.agent_uuid is not null
and sdh.msg.created_at >= '2021-05-20')

select * from a where agent_name is not null 
--Order by created_at
