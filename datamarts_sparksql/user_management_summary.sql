select
    pt_date,
    main.user_id,
    service_week,
    week_start_date,
    week_end_date,
    phase_no,
    weekly_goal_ft,
    weekly_goal_aft_bf,
    weekly_goal_wg,
    avg_fasting,
    avg_after_breakfast,
    measure_value as avg_weight,
    weight_indicator,
    user_tag,
    user_read
from (
    select 
        distinct
        pt_date,
        a.user_id,
        a.service_week,
        a.user_read,
        date(b.service_start_date + 7*(a.service_week -1)) as week_start_date,
        date(b.service_start_date + 7*(a.service_week -1) +6 ) as week_end_date,
        b.cbt_phase_no as phase_no,
        a.weekly_goal_ft,
        a.weekly_goal_aft_bf,
        a.weekly_goal_wg,
        a.avg_fasting,
        a.avg_after_breakfast,
        b.user_tag
    from (
        select
            pt_date,
            user_id,
            service_week,
            user_read,
            concat(glucose_0min_this_week_tvl* 18, ' - ', glucose_0min_this_week_tvh* 18 ) as weekly_goal_ft,
            concat(glucose_120min_this_week_tvl* 18, ' - ', glucose_120min_this_week_tvh* 18 ) as weekly_goal_aft_bf,
            concat(round(weight_this_week_tvl* 2.2,2), ' - ', round(weight_this_week_tvh * 2.2,2) ) as weekly_goal_wg,
            glucose_0min_avg_this_week * 18 as avg_fasting,
            glucose_120min_avg_this_week * 18 as avg_after_breakfast
            -- weight_week_goal as avg_weight
        from hal_report_summary_weekly_daily_record
        where pt_date = '2022-05-29'
    ) a
    left join (
        select 
            user_id, 
            week_no,
            service_start_date,
            cbt_phase_no,
            max(user_tag) user_tag
        from hal_report_user_record
        group by user_id, week_no
        
    ) b
    on a.user_id = b.user_id
    and a.service_week = b.week_no

) main
left join (
    select 
        a.user_id,
        a.week_no,
        a.measure_date,
        a.measure_value,
        b.baseline,
        case
            when a.measure_value < baseline then "Yes"
            else "No"
        end as weight_indicator
    from (
        select
            main.user_id,
            main.week_no,
            sub.measure_date,
            sub.measure_value
        from (
            select 
                a.user_id, week_no, max(measure_date) max_date
            from  (
                select *, date(measure_time) as measure_date
                from hal_report_medical_indication_records
                where pt_date = '2022-05-29'
                and indication_name_en = 'weight'
                and record_type = 1
                and is_effective = 1
            ) a 
            inner join  (
                select 
                    distinct
                    user_id, 
                    week_no,
                    date(service_start_date + 7*(week_no -1)) as week_start_date,
                    date(service_start_date + 7*(week_no -1) +6 ) as week_end_date
                from hal_report_user_record
            )b
            on a.user_id = b.user_id
            and a.measure_date >=  b.week_start_date and a.measure_date <= b.week_end_date
            group by a.user_id, week_no
        ) main
        inner join (
            select user_id, date(measure_time) as measure_date, measure_value
            from hal_report_medical_indication_records
            where pt_date = '2022-05-29'
            and indication_name_en = 'weight'
            and record_type = 1
            and is_effective = 1
    
        ) sub
        on main.user_id = sub.user_id
        and main.max_date = sub.measure_date
    ) a
    left join (
        select
            main.user_id,
            main.week_no,
            sub.measure_value as baseline
        from (
            select 
                a.user_id, week_no, max(measure_date) max_date
            from  (
                select *, date(measure_time) as measure_date
                from hal_report_medical_indication_records
                where pt_date = '2022-05-29'
                and indication_name_en = 'weight'
                and record_type = 1
                and is_effective = 1
            ) a 
            inner join  (
                select 
                    distinct
                    user_id, 
                    week_no,
                    date(service_start_date + 7*(week_no -1)) as week_start_date,
                    date(service_start_date + 7*(week_no -1) +6 ) as week_end_date
                from hal_report_user_record
            )b
            on a.user_id = b.user_id
            and a.measure_date >=  b.week_start_date and a.measure_date <= b.week_end_date
            group by a.user_id, week_no
        ) main
        inner join (
            select user_id, date(measure_time) as measure_date, measure_value
            from hal_report_medical_indication_records
            where pt_date = '2022-05-29'
            and indication_name_en = 'weight'
            and record_type = 1
            and is_effective = 1
    
        ) sub
        on main.user_id = sub.user_id
        and main.max_date = sub.measure_date
        union all 
        --baseline
        select user_id, 0 as week_no, measure_value
        from hal_report_medical_indication_records
        where pt_date = '2022-05-29'
        and indication_name_en = 'weight'
        and record_type = 10
    ) b
    on a.user_id = b.user_id
    and a.week_no = b.week_no+1
 

) c
on main.user_id = c.user_id
and main.service_week = c.week_no


order by main.user_id, week_start_date