select distinct 
    pt_date,
    main.user_id, 
    week_no, 
    plan_date, 
    main.timing, 
    sub.recommend_carbohydrate,
    case 
        when 
            main.user_id = sub.user_id and 
            main.plan_date = sub.record_date and 
            main.timing = sub.timing
        then "Yes"
        else "No"
    end as meal_status,
    sub.record_carbohydrate,
    case 
        when
            main.user_id = sub.user_id and 
            main.plan_date = sub.record_date and 
            main.timing = sub.timing and
            record_carbohydrate between recommend_carbohydrate -7.5 and recommend_carbohydrate +7.5
        then"Yes"
        else "No"
    end as carb_within_recommended,
    date_format(sub.origin_update_time, "H:m:s") as update_time
from(
    select 
        a.user_id, 
        a.week_no, 
        plan_date, 
        plan_indicator_type as timing,
        pt_date
    from(
        select distinct user_id, week_no, pt_date
        from hal_report.user_record
        where pt_date = '2022-05-22'
    ) a
    inner join(

        select 
            user_id, week_no, plan_date, 
            max(case when glucose_120min = 1 then 'breakfast' else 'na' end) as plan_indicator_type
        from hal_report.monitor_plan_record
        where pt_date = '2022-05-22' and plan_date between '2022-05-16' and '2022-05-22'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when rpg2 = 1 then 'lunch' else 'na' end) as plan_indicator_type
        from hal_report.monitor_plan_record
        where pt_date = '2022-05-22' and plan_date between '2022-05-16' and '2022-05-22'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'  
        union all
        select 
            user_id, week_no, plan_date, 
            max(case when rpg4 = 1 then 'dinner' else 'na' end) as plan_indicator_type
        from hal_report.monitor_plan_record
        where pt_date = '2022-05-22' and plan_date between '2022-05-16' and '2022-05-22'
        group by user_id, week_no, plan_date
        having plan_indicator_type <> 'na'  
    ) b 
    on a.user_id = b.user_id
) main
left join(
    select user_id, record_date, timing, 
        sum(record_carbohydrate) as record_carbohydrate, 
        max(recommend_carbohydrate) as recommend_carbohydrate, 
        max(origin_update_time) as origin_update_time
    from hal_report.diet_daily_record
    where pt_date = '2022-05-22' and record_date between '2022-05-16' and '2022-05-22'
    group by user_id, record_date, timing

) sub
on main.user_id = sub.user_id 
and main.plan_date = sub.record_date 
and main.timing = sub.timing
order by main.user_id, plan_date