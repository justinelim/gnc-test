select
    a.user_id,
    a.week_no,
    a.week_start_date,
    a.week_end_date,
    a.phase_no,
    h.num_app_logins,
    c.num_medical_records,
    concat(c.monitoring_prog_rate,'%') monitoring_prog_rate,
    d.num_diet_records,
    concat(d.diet_completion_rate,'%') diet_completion_rate,
    e.num_exercise_records,
    concat(e.exercise_completion_rate,'%') exercise_completion_rate,
    f.14_challenge_start_time,
    f.14_challenge_end_time,
    concat(f.14_challenge_completion_rate,'%') 14_challenge_completion_rate,
    concat(g.course_learning_progress_rate,'%') course_learning_progress_rate
    
    from (
        select distinct
            a.user_id,
            a.week_no,
            week_start_date,
            week_end_date,
            a.cbt_phase_no as phase_no
        from (
            select
                user_id,
                week_no,
                service_start_date,
                date(service_start_date + 7*(week_no -1)) as week_start_date,
                date(service_start_date + 7*(week_no -1) +6 ) as week_end_date,cbt_phase_no
            from hal_report_user_record
            where pt_date <= '")&text(i2,"yyyy-mm-dd")&lower("'
        ) a
        left join (
            select
                user_id,
                service_week
            from hal_report_summary_weekly_daily_record
            where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
            
        ) b
        on a.user_id = b.user_id
    ) a
    left join (
        select
        main.user_id,
        main.week_no,
        num_medical_records,
        round(num_medical_records/monitor_total*100,2) as monitoring_prog_rate
    from (
        select 
            a.user_id,
            week_no,
            count(id) as num_medical_records
        from (
            select user_id, id, date(measure_time) as measure_date
            from hal_report_medical_indication_records
            where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
            and indication_sub_name in ('fasting', 'after_breakfast', 'before_lunch', 'after_lunch', 'before_dinner', 'after_dinner', 'before_sleep', 'am_3')
            and is_effective = 1
            and record_type <> 10
        ) a
        inner join(
            select distinct
                user_id,
                week_no,
                date(service_start_date + 7*(week_no -1)) as week_start_date,
                date(service_start_date + 7*(week_no -1) +6 ) as week_end_date
            from hal_report_user_record
            where pt_date <= '")&text(i2,"yyyy-mm-dd")&lower("'
            
        )b
        on a.user_id = b.user_id
        and a.measure_date >=  b.week_start_date and a.measure_date <= b.week_end_date
        group by a.user_id, week_no
        ) main
        inner join(
            select
            user_id,
            week_no,
            sum(glucose_0min + glucose_120min + rpg1 + rpg2 + rpg3 + rpg4 + rpg5)  as monitor_total
        from hal_report_monitor_plan_record
        where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
        group by user_id, week_no
    ) sub
    on main.user_id = sub.user_id
    and main.week_no = sub.week_no
) c
on a.user_id = c.user_id
and a.week_no = c.week_no
left join (
    select
        user_id,
        week_no,
        count(*) as num_diet_records,
        round(count(*) / 9 * 100,2) as diet_completion_rate
    from (
        select
            a.user_id,
            b.week_no
        from (
            select distinct
                user_id,
                timing,
                date(record_date) as record_date
            from hal_report_diet_daily_record
            where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
        ) a
        inner join(
            select distinct
                user_id,
                week_no,
                date(service_start_date + 7*(week_no -1)) as week_start_date,
                date(service_start_date + 7*(week_no -1) +6 ) as week_end_date
            from hal_report_user_record
            where pt_date <= '")&text(i2,"yyyy-mm-dd")&lower("'
        ) b
        on a.user_id = b.user_id
        and a.record_date >=  b.week_start_date and a.record_date <= b.week_end_date
                            
        order by a.user_id
    ) grp
    group by user_id, week_no
) d
on a.user_id = d.user_id
and a.week_no = d.week_no
left join(
    select
        a.user_id,
        week_no,
        count(*) as num_exercise_records,
        round(sum(train_clock_timelength)/60/150*100,2) as exercise_completion_rate
    from (
        select
            user_id,
            train_clock_timelength,
            date(train_clock_date) as train_clock_date
        from hal_report_train_execution_record
        where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
    ) a
    inner join (
        select distinct
            user_id,
            week_no,
            date(service_start_date + 7*(week_no -1)) as week_start_date,
            date(service_start_date + 7*(week_no -1) +6 ) as week_end_date
        from hal_report_user_record
        where pt_date <= '")&text(i2,"yyyy-mm-dd")&lower("'
    ) b
    on a.user_id = b.user_id
    and a.train_clock_date >=  b.week_start_date and a.train_clock_date <= b.week_end_date
    
    group by a.user_id, week_no
) e
on a.user_id = e.user_id
and a.week_no = e.week_no
left join(
    select
        e.user_id,
        e.week_no,
        e.week_start_date,
        e.week_end_date,
        e.14_challenge_start_time,
        e.14_challenge_end_time,
        total_updates,
        total_target_updates,
        case when num_challenge_days <= 14 then round(total_updates/total_target_updates*100,2)
        else progress end as 14_challenge_completion_rate
        
    from (
        select
            user_id,
            week_no,
            14_challenge_start_time,
            14_challenge_end_time,
            num_challenge_days,
            week_start_date,
            week_end_date,
            sum(total_target_updates) as total_target_updates
    from (
        select
            user_id,
            week_no,
            date(target_start_date) 14_challenge_start_time,
            date(target_end_date) 14_challenge_end_time,
            num_challenge_days,
            week_start_date,
            week_end_date,
            case
                when target_check_frequency = 'per meal' then num_challenge_days * 3
                when target_check_frequency = 'per day' then num_challenge_days * 1
                when target_check_frequency = 'per week' then left(num_challenge_days/7,1) * 1
                else null
            end as total_target_updates
        from (
            select
                a.user_id,
                a.week_no,
                target_check_frequency,
                case
                when target_check_frequency = 'per week' then 1
                when target_check_frequency = 'per day' then 1
                when target_check_frequency = 'per meal' then 3
                else null
                end as total_target_updates,
                date(target_start_date) as target_start_date,
                date(target_end_date) as target_end_date,
                datediff(week_end_date, target_start_date) + 1 as num_challenge_days,
                week_start_date,
                week_end_date
            from (
                select distinct
                    user_id,
                    week_no,
                    cbt_phase_no,
                    date(service_start_date + 7*(week_no -1)) as week_start_date,
                    date(service_start_date + 7*(week_no -1) +6 ) as week_end_date
                from hal_report_user_record
                where pt_date <= '")&text(i2,"yyyy-mm-dd")&lower("'
                ) a
                left join (
                    select user_id
                    from  hal_report_course_record
                    where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
                    and lower(course_name) like '%goals: the steps to success%' and answer_correct = 1
                ) b
                on a.user_id = b.user_id
                right join (
                    select user_id, target_no, target_type,target_start_date,target_item_name_en,
                    bt_title,target_check_frequency,target_end_date, pt_date
                from  hal_report_challenge_target_record
                where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
                ) c
                on a.user_id = c.user_id
                and (c.target_start_date <=  a.week_end_date and c.target_end_date >= a.week_start_date)
            ) main
        ) d
        group by user_id, week_no,14_challenge_start_time
        ) e
    right join (
        select a.user_id, week_no, week_start_date,target_start_date as 14_challenge_start_time, count(*) total_updates from (
            select a.user_id, target_start_date, target_end_date, check_date from (
                select distinct
                    user_id,
                    date(target_start_date) target_start_date,
                    date(target_end_date) target_end_date
                from hal_report_challenge_target_record
                where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
            ) a
            inner join(
                select
                    user_id, date(check_datetime) check_date
                from hal_report_challenge_checklist_record
                where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
            ) b
            on a.user_id = b.user_id
            and b.check_date >=  a.target_start_date and b.check_date <= a.target_end_date
        ) a
        left join (
            select distinct
                user_id,
                week_no,
                date(service_start_date + 7*(week_no -1)) as week_start_date,
                date(service_start_date + 7*(week_no -1) +6 ) as week_end_date
            from hal_report_user_record
            where pt_date <= '")&text(i2,"yyyy-mm-dd")&lower("'
        ) b
        on a.user_id = b.user_id
        and(a.target_start_date <=  b.week_end_date and a.target_end_date >= b.week_start_date)
        and(a.check_date >=  a.target_start_date and a.check_date <= b.week_end_date)
        group by a.user_id, week_no, week_start_date, target_start_date
    ) f
    on e.user_id = f.user_id
    and e.week_no = f.week_no
    and e.week_start_date = f.week_start_date
    and e.14_challenge_start_time = f.14_challenge_start_time
    
    left join (
        select
            user_id, json_extract(result, '$.progress') as progress
        from hal_report_challenge_state_record
        where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
        ) g
        on f.user_id = g.user_id
                    
        order by user_id, e.week_no
    ) f
    on a.user_id = f.user_id
    and a.week_no = f.week_no
    left join (
        select
            a.user_id,
            a.week_no,
            round(sum(tests_indicator)/sum(unlock_indicator)*100,2) as course_learning_progress_rate
    from (
        select
            pt_date,
            user_id,
            week_no,
            phase_no,
            service_start_date,
            service_num_days,
            course_id,
            course_name,
            unlock_indicator,
            case when unlock_indicator = 0 then 0
                else tests_indicator
            end as tests_indicator
        from (
            select
                pt_date,
                main.user_id,
                week_no,
                cbt_phase_no as phase_no,
                cbt_phase,
                phase_day,
                service_start_date,
                service_num_days,
                course_id,
                sub.course_name,
                tests_indicator,
                case
                    when cbt_phase < cbt_phase_no then 1
                    when cbt_phase = cbt_phase_no and phase_day <= service_num_days then 1
                    else 0
                end as unlock_indicator
            from (
                select user_id, pt_date, week_no, cbt_phase_no, service_start_date,case
                when cbt_phase_no = 1 then pt_date - service_start_date
                when cbt_phase_no = 2 then pt_date - service_start_date - 14
                else null
                end as service_num_days
                from (
                    select distinct user_id, week_no, pt_date, cbt_phase_no, service_start_date
                    from hal_report_user_record
                    where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
                ) a
            ) main
            left join (
                select
                    user_id, course_id, course_name,
                    max(tests_indicator) as tests_indicator
                from (
                    select
                        user_id, course_id, course_name,
                        case
                            when learn_time is null then 0
                            when has_quiz = 1 and answer_correct >=1 then 1
                            when has_quiz = 0 and learn_time is not null then 1
                            else 0
                        end as tests_indicator
                    from hal_report_course_record
                    where pt_date = '")&text(i2,"yyyy-mm-dd")&lower("'
                    order by user_id, course_id
                ) grp
                group by user_id, course_id
            ) sub
            on main.user_id = sub.user_id
            left join(
                select course_no, cbt_phase, phase_day from hal_business_report_course_listing
            ) sub2
            on sub.course_id = sub2.course_no
        ) final
    ) a
    group by a.user_id, week_no
    
    ) g
    on a.user_id = g.user_id
    and a.week_no = g.week_no
    left join(
        select
            user_id,
            week_no,
            count(*) as num_app_logins
        from (
            select
                a.user_id,
                a.day,
                b.week_no,
                week_start_date,
                week_end_date
            from (
                select 
                    ual.id,
                    date(ual.create_time) as day,
                    ual.user_id
                from hal_report_white_user as wu
                inner join hal_usercenter_user_action_log as ual
                on ual.user_id = wu.user_id
            ) a

            inner join(
                select distinct
                    user_id, 
                    week_no,
                    date(service_start_date + 7*(week_no -1)) as week_start_date,
                    date(service_start_date + 7*(week_no -1) +6 ) as week_end_date
                from hal_report_user_record
                where pt_date <= '")&text(i2,"yyyy-mm-dd")&lower("'
            ) b
            on a.user_id = b.user_id
            and a.day >=  b.week_start_date and a.day <= b.week_end_date
        ) temp
        group by user_id, week_no
        order by user_id, day
    ) h
    on a.user_id = h.user_id
    and a.week_no = h.week_no
    order by user_id, week_no