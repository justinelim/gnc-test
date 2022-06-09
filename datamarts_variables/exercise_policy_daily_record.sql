select distinct 
    pt_date,
    main.user_id, 
    main.week_no, 
    plan_date, 
    exercise_type,
    training_schema_id,
    exercise_timelength,
    exercise_hr,
    case 
        when main.user_id = sub.user_id and 
            main.plan_date = sub.train_clock_date and 
            main.exercise_type = sub.training_schema_type
        then "Yes"
        else "No"
    end as exercise_status,
    record_type,
    record_timelength,
    case 
        when main.user_id = sub.user_id and 
            main.plan_date = sub.train_clock_date and 
            main.exercise_type = sub.training_schema_type and 
            record_timelength >= exercise_timelength
        then "Yes"
        else "No"
    end as timelength_indicator,
    record_hr,
    case 
        when main.user_id = sub.user_id and 
            main.plan_date = sub.train_clock_date and 
            main.exercise_type = sub.training_schema_type and 
            sub.record_hr >= hr_low
        then "Yes"
        else "No"
    end as heartrate_indicator,
    date_format(sub.update_time, "H:m:s") as update_time
    
from (
    select 
        a.user_id, 
        a.week_no, 
        plan_date, 
        exercise_type,
        training_schema_id,
        exercise_timelength,
        exercise_hr,
        hr_low,
        hr_high,
        pt_date
    from (
        select distinct user_id, week_no, pt_date
        from hal_report_user_record
        where pt_date = '2022-05-22'
    ) a
    inner join (
        select 
            user_id, 
            plan_date,
            training_schema_type as exercise_type, 
            training_schema_id,
            training_timelength as exercise_timelength, 
            concat(hr_low, ' - ', hr_high) as exercise_hr,
            hr_low,
            hr_high
        from hal_report_exercise_policy_daily_record
        where pt_date = '2022-05-22' and training_schema_type <> 'rest'
        and plan_date between '2022-05-16' and '2022-05-22'
    ) b 
    on a.user_id = b.user_id
) main
left join (
    select 
        user_id,
        train_clock_date,
        training_schema_type,
        max(record_type) as record_type,
        max(record_timelength) as record_timelength,
        max(record_hr) as record_hr,
        max(update_time) as update_time
    from (
        select 
            user_id,
            date(train_clock_date) as train_clock_date,
            case 
                when type = 1 then "training"
                when type = 2 then "others"
                else null
            end as record_type,
            round(train_clock_timelength/60,0) as record_timelength,
            train_clock_hr as record_hr,
            update_time,
            training_schema_type
        from hal_report_train_execution_record
        where pt_date = '2022-05-22'
        and date(train_clock_date) between '2022-05-16' and '2022-05-22'
    )  grped
    group by user_id, train_clock_date, training_schema_type, record_type

) sub
on main.user_id = sub.user_id 
and main.plan_date = sub.train_clock_date 
and main.exercise_type = sub.training_schema_type
order by main.user_id, plan_date