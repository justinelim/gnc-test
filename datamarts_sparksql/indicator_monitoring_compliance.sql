select distinct 
    pt_date,
    main.user_id,
    week_no,
    service_group,
    plan_date,
    plan_indicator_type,
    case 
        when sub.update_date between '2022-05-23' and '2022-05-29' and main.user_id = sub.user_id and main.plan_date = sub.update_date and main.plan_indicator_type = sub.indication_sub_name
        then "Yes"
        else "No"
    end as indicator_status,
    date_format(sub.update_time, "H:m:s") as update_time
from (
    select a.user_id, a.week_no, service_group, plan_date, plan_indicator_type, pt_date
    from (
        select distinct user_id, week_no, service_group, pt_date 
        from hal_report_user_record
        where pt_date = '2022-05-29'
    ) a
    inner join(
        select 
            user_id, week_no, plan_date, 
            max(case when glucose_0min = 1 then 'fasting' else 'na' end) as plan_indicator_type
        from hal_report_monitor_plan_record
        where pt_date = '2022-05-29' and plan_date between '2022-05-23' and '2022-05-29'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when glucose_120min = 1 then 'after_breakfast' else 'na' end) as plan_indicator_type
        from hal_report_monitor_plan_record
        where pt_date = '2022-05-29' and plan_date between '2022-05-23' and '2022-05-29'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when rpg1 = 1 then 'before_lunch' else 'na' end) as plan_indicator_type
        from hal_report_monitor_plan_record
        where pt_date = '2022-05-29' and plan_date between '2022-05-23' and '2022-05-29'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'  
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when rpg2 = 1 then 'after_lunch' else 'na' end) as plan_indicator_type
        from hal_report_monitor_plan_record
        where pt_date = '2022-05-29' and plan_date between '2022-05-23' and '2022-05-29'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'  
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when rpg3 = 1 then 'before_dinner' else 'na' end) as plan_indicator_type
        from hal_report_monitor_plan_record
        where pt_date = '2022-05-29' and plan_date between '2022-05-23' and '2022-05-29'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'  
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when rpg4 = 1 then 'after_dinner' else 'na' end) as plan_indicator_type
        from hal_report_monitor_plan_record
        where pt_date = '2022-05-29' and plan_date between '2022-05-23' and '2022-05-29'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'  
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when rpg5 = 1 then 'before_sleep' else 'na' end) as plan_indicator_type
        from hal_report_monitor_plan_record
        where pt_date = '2022-05-29' and plan_date between '2022-05-23' and '2022-05-29'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'  
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when weight = 1 then 'weight' else 'na' end) as plan_indicator_type
        from hal_report_monitor_plan_record
        where pt_date = '2022-05-29' and plan_date between '2022-05-23' and '2022-05-29'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'  
    ) b 
    on a.user_id = b.user_id
) main
left join(
    select user_id, indication_sub_name, date(update_time) as update_date, max(update_time) as update_time 
    from hal_report_medical_indication_records
    where pt_date = '2022-05-29' and record_type != 10 and is_effective =1  
    and date(update_time) between '2022-05-23' and '2022-05-29'
    group by user_id, indication_sub_name, date(update_time)

) sub
on main.user_id = sub.user_id 
and main.plan_date = sub.update_date 
and main.plan_indicator_type = sub.indication_sub_name
order by main.user_id, plan_date