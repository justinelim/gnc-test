select
    distinct
    pt_date,
    user_id,
    week_no,
    phase_no,
    service_start_date,
    login_indicator,
    update_indicator,
    login_indicator_7,
    update_indicator_7,
    update_cont_neg_7,
    update_cont_neg_14,
    update_cont_neg_28,
    phase2_login_indicator_7,
    phase2_update_indicator_7
    
from (
    select 
        main.user_id, 
        case when main.use_app = 1 then "Yes"
            when main.use_app = 0 then "No"
            else null 
        end as login_indicator, 
        main.service_start_date, 
        week_no,
        phase_no,
        main.pt_date, 
        -- pt_date_interval,
        case when day1.update_date is not null then "Yes"
            else "No"
        end as update_indicator,
        case when day7_log.user_id is not null then "Yes"
            else "No"
        end as login_indicator_7,
        case when day7_upd.user_id is not null then "Yes"
            else "No"
        end as update_indicator_7,
        case when day7_upd_cont.num_days_interval >= 7 and day7_upd_cont.cnt = 0 then "Yes"
            else "No"
        end as update_cont_neg_7,
        case when day14_upd_cont.num_days_interval >= 14 and day14_upd_cont.cnt = 0 then "Yes"
            else "No"
        end as update_cont_neg_14,
        case when day28_upd_cont.num_days_interval >= 28 and day28_upd_cont.cnt = 0 then "Yes"
            else "No"
        end as update_cont_neg_28,
        case when p2day7_log.user_id is not null then "Yes"
            else "No"
        end as phase2_login_indicator_7,
        case when p2_day7_upd.user_id is not null then "Yes"
            else "No"
        end as phase2_update_indicator_7
    from (
        select 
            user_id,
            pt_date,
            week_no,
            cbt_phase_no as phase_no,
            service_start_date,
            use_app,
            DATE_ADD(pt_date, INTERVAL 0 DAY) as pt_date_interval,
            DATE_ADD(service_start_date, INTERVAL 7 DAY) as service_start_date_7,
            DATE_ADD(service_start_date, INTERVAL 14 DAY) as service_start_date_14,
            DATE_ADD(service_start_date, INTERVAL 20 DAY) as service_start_date_20
        from hal_report_user_record
        
    ) main
    left join (
        select distinct *
        from (
            select user_id, date(update_time) as update_date
            from hal_report_medical_indication_records
            where is_effective = 1 and record_type <> 10
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_diet_daily_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_train_execution_record
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_target_record
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_checklist_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_order_daily_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_plan_record
        ) i_a

    ) day1
    on main.user_id = day1.user_id
    and main.pt_date = day1.update_date
    
    left join (
        select distinct user_id, date(update_time) as update_date, use_app
        from hal_report_user_record
        where use_app = 1
        
    ) day7_log
    on main.user_id = day7_log.user_id
    and day7_log.update_date between main.service_start_date and main.service_start_date_7
    and day7_log.update_date >= main.service_start_date 
    
    left join (
        select distinct *
        from (
            select user_id, date(update_time) as update_date
            from hal_report_medical_indication_records
            where is_effective = 1 and record_type <> 10
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_diet_daily_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_train_execution_record
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_target_record
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_checklist_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_order_daily_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_plan_record
        ) i_a
    
    ) day7_upd
    on main.user_id = day7_upd.user_id
    and day7_upd.update_date between main.service_start_date and main.service_start_date_7
    and day7_upd.update_date >= main.service_start_date
    
    left join (
        select i_a.user_id, pt_date, service_start_date, num_days_interval, count(update_date) as cnt  from (
            select 
                user_id,
                pt_date,
                week_no,
                service_start_date,
                DATE_ADD(pt_date, INTERVAL -7 DAY) as pt_date_7,
                DATEDIFF(pt_date, service_start_date) as num_days_interval
            from hal_report_user_record
        ) i_a 
        left join (
            select distinct user_id, update_date
            from (
                select user_id, date(update_time) as update_date
                from hal_report_medical_indication_records
                where is_effective = 1 and record_type <> 10
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_diet_daily_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_train_execution_record
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_challenge_target_record
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_challenge_checklist_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_medication_order_daily_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_medication_plan_record
            ) i_c
        ) i_b
        on i_a.user_id = i_b.user_id
        and i_b.update_date between i_a.pt_date_7 and i_a.pt_date
        and i_b.update_date >= i_a.service_start_date 
        group by i_a.user_id, pt_date, service_start_date

    ) day7_upd_cont
    on main.user_id = day7_upd_cont.user_id
    and main.pt_date = day7_upd_cont.pt_date
    and main.service_start_date = day7_upd_cont.service_start_date
    
    left join (
        select i_a.user_id, pt_date, service_start_date, num_days_interval, count(update_date) as cnt  from (
            select 
                user_id,
                pt_date,
                week_no,
                service_start_date,
                DATE_ADD(pt_date, INTERVAL -14 DAY) as pt_date_14,
                DATEDIFF(pt_date, service_start_date) as num_days_interval
            from hal_report_user_record
        ) i_a 
        left join (
            select distinct user_id, update_date
            from (
                select user_id, date(update_time) as update_date
                from hal_report_medical_indication_records
                where is_effective = 1 and record_type <> 10
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_diet_daily_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_train_execution_record
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_challenge_target_record
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_challenge_checklist_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_medication_order_daily_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_medication_plan_record
            ) i_c
        ) i_b
        on i_a.user_id = i_b.user_id
        and i_b.update_date between i_a.pt_date_14 and i_a.pt_date
        and i_b.update_date >= i_a.service_start_date 
        group by i_a.user_id, pt_date, service_start_date

    ) day14_upd_cont
    on main.user_id = day14_upd_cont.user_id
    and main.pt_date = day14_upd_cont.pt_date
    and main.service_start_date = day14_upd_cont.service_start_date
    
    left join (
        select i_a.user_id, pt_date, service_start_date, num_days_interval, count(update_date) as cnt  from (
            select 
                user_id,
                pt_date,
                week_no,
                service_start_date,
                DATE_ADD(pt_date, INTERVAL -28 DAY) as pt_date_28,
                DATEDIFF(pt_date, service_start_date) as num_days_interval
            from hal_report_user_record
        ) i_a 
        left join (
            select distinct user_id, update_date
            from (
                select user_id, date(update_time) as update_date
                from hal_report_medical_indication_records
                where is_effective = 1 and record_type <> 10
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_diet_daily_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_train_execution_record
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_challenge_target_record
                union all
                
                select user_id, date(origin_update_time) as update_date
                from hal_report_challenge_checklist_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_medication_order_daily_record
                union all
                
                select user_id, date(update_time) as update_date
                from hal_report_medication_plan_record
            ) i_c
        ) i_b
        on i_a.user_id = i_b.user_id
        and i_b.update_date between i_a.pt_date_28 and i_a.pt_date
        and i_b.update_date >= i_a.service_start_date 
        group by i_a.user_id, pt_date, service_start_date

    ) day28_upd_cont
    on main.user_id = day28_upd_cont.user_id
    and main.pt_date = day28_upd_cont.pt_date
    and main.service_start_date = day28_upd_cont.service_start_date
    
    left join (
        select distinct user_id, date(update_time) as update_date, use_app
        from hal_report_user_record
        where use_app = 1
        
    ) p2day7_log
    on main.user_id = p2day7_log.user_id
    and p2day7_log.update_date between main.service_start_date_14 and main.service_start_date_20
    and p2day7_log.update_date >= main.service_start_date 
    
    left join (
        select distinct *
        from (
            select user_id, date(update_time) as update_date
            from hal_report_medical_indication_records
            where is_effective = 1 and record_type <> 10
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_diet_daily_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_train_execution_record
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_target_record
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_checklist_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_order_daily_record
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_plan_record
        ) i_a
    
    ) p2_day7_upd
    on main.user_id = p2_day7_upd.user_id
    and p2_day7_upd.update_date between main.service_start_date_14 and main.service_start_date_20
    and p2_day7_upd.update_date >= main.service_start_date
    
) final
order by pt_date, week_no, user_id