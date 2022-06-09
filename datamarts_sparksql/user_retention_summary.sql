select 
    main.service_start_date as date,
    num_new_users,
    case when num_retained_users_7 is null then 0 else num_retained_users_7 end as num_retained_users_7,
    case when num_retained_users_14 is null then 0 else num_retained_users_14 end as num_retained_users_14,
    case when num_retained_users_21 is null then 0 else num_retained_users_21 end as num_retained_users_21,
    case when num_retained_users_28 is null then 0 else num_retained_users_28 end as num_retained_users_28
from (
    select 
        service_start_date,
        DATE_ADD(service_start_date, INTERVAL 7 DAY) as service_start_date_7,
        count(*) as num_new_users 
    from (
        select distinct user_id, service_start_date
        from hal_report_user_record
    ) grp 
    group by service_start_date
    order by service_start_date
) main
left join (
    select service_start_date, service_start_date_7, sum(indicator) as num_retained_users_7 from (
        select 
            distinct i_a.user_id, service_start_date, service_start_date_7,
            case when i_a.user_id is not null then 1
            else 0 end as indicator
        from (
            select user_id, date(update_time) as update_date
            from hal_report_medical_indication_records
            -- where pt_date = '2022-05-22'
            where is_effective = 1 and record_type <> 10
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_diet_daily_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_train_execution_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_target_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_checklist_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_order_daily_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_plan_record
            -- where pt_date = '2022-05-22
        ) i_a
        inner join (
            select 
                distinct
                user_id,
                service_start_date,
                DATE_ADD(service_start_date, INTERVAL 7 DAY) as service_start_date_7
            from (
                select distinct user_id, service_start_date
                from hal_report_user_record
            ) grp 
            group by user_id, service_start_date
        ) i_b
        on i_a.user_id = i_b.user_id
        and i_a.update_date between i_b.service_start_date and i_b.service_start_date_7
        and i_a.update_date >= i_b.service_start_date
        order by i_b.user_id, service_start_date

    ) i_c
    group by service_start_date, service_start_date_7

) 7days
on main.service_start_date = 7days.service_start_date
left join (
    select service_start_date, service_start_date_14, sum(indicator) as num_retained_users_14 from (
        select 
            distinct i_a.user_id, service_start_date, service_start_date_14,
            case when i_a.user_id is not null then 1
            else 0 end as indicator
        from (
            select user_id, date(update_time) as update_date
            from hal_report_medical_indication_records
            -- where pt_date = '2022-05-22'
            where is_effective = 1 and record_type <> 10
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_diet_daily_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_train_execution_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_target_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_checklist_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_order_daily_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_plan_record
            -- where pt_date = '2022-05-22
        ) i_a
        inner join (
            select 
                distinct
                user_id,
                service_start_date,
                DATE_ADD(service_start_date, INTERVAL 8 DAY) as service_start_date_8,
                DATE_ADD(service_start_date, INTERVAL 14 DAY) as service_start_date_14
            from (
                select distinct user_id, service_start_date
                from hal_report_user_record
            ) grp 
            group by user_id, service_start_date
        ) i_b
        on i_a.user_id = i_b.user_id
        and i_a.update_date between i_b.service_start_date_8 and i_b.service_start_date_14
        and i_a.update_date >= i_b.service_start_date_8
        order by i_b.user_id, service_start_date

    ) i_c
    group by service_start_date, service_start_date_14

) 14days
on main.service_start_date = 14days.service_start_date
left join (
    select service_start_date, service_start_date_21, sum(indicator) as num_retained_users_21 from (
        select 
            distinct i_a.user_id, service_start_date, service_start_date_21,
            case when i_a.user_id is not null then 1
            else 0 end as indicator
        from (
            select user_id, date(update_time) as update_date
            from hal_report_medical_indication_records
            -- where pt_date = '2022-05-22'
            where is_effective = 1 and record_type <> 10
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_diet_daily_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_train_execution_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_target_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_checklist_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_order_daily_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_plan_record
            -- where pt_date = '2022-05-22
        ) i_a
        inner join (
            select 
                distinct
                user_id,
                service_start_date,
                DATE_ADD(service_start_date, INTERVAL 15 DAY) as service_start_date_15,
                DATE_ADD(service_start_date, INTERVAL 21 DAY) as service_start_date_21
            from (
                select distinct user_id, service_start_date
                from hal_report_user_record
            ) grp 
            group by user_id, service_start_date
        ) i_b
        on i_a.user_id = i_b.user_id
        and i_a.update_date between i_b.service_start_date_15 and i_b.service_start_date_21
        and i_a.update_date >= i_b.service_start_date_15
        order by i_b.user_id, service_start_date

    ) i_c
    group by service_start_date, service_start_date_21

) 21days
on main.service_start_date = 21days.service_start_date
left join (
    select service_start_date, service_start_date_28, sum(indicator) as num_retained_users_28 from (
        select 
            distinct i_a.user_id, service_start_date, service_start_date_28,
            case when i_a.user_id is not null then 1
            else 0 end as indicator
        from (
            select user_id, date(update_time) as update_date
            from hal_report_medical_indication_records
            -- where pt_date = '2022-05-22'
            where is_effective = 1 and record_type <> 10
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_diet_daily_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_train_execution_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_target_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(origin_update_time) as update_date
            from hal_report_challenge_checklist_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_order_daily_record
            -- where pt_date = '2022-05-22'
            union all
            
            select user_id, date(update_time) as update_date
            from hal_report_medication_plan_record
            -- where pt_date = '2022-05-22'
        ) i_a
        inner join (
            select 
                distinct
                user_id,
                service_start_date,
                DATE_ADD(service_start_date, INTERVAL 15 DAY) as service_start_date_22,
                DATE_ADD(service_start_date, INTERVAL 21 DAY) as service_start_date_28
            from (
                select distinct user_id, service_start_date
                from hal_report_user_record
            ) grp 
            group by user_id, service_start_date
        ) i_b
        on i_a.user_id = i_b.user_id
        and i_a.update_date between i_b.service_start_date_22 and i_b.service_start_date_28
        and i_a.update_date >= i_b.service_start_date_22
        order by i_b.user_id, service_start_date

    ) i_c
    group by service_start_date, service_start_date_28

) 28days
on main.service_start_date = 28days.service_start_date