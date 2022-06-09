select 
    pt_date,
    user_id,
    week_no,
    phase_no,
    service_start_date,
    service_num_days,
    course_id,
    course_name,
    course_type,
    unlock_indicator,
    learn_indicator,
    learn_time,
    num_learn_time,
    quiz_indicator,
    num_tests,
    case when unlock_indicator = "No" then "No"
        else tests_indicator 
    end as tests_indicator,
    test_time
from(
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
        course_type,
        learn_indicator,
        learn_time,
        num_learn_time,
        quiz_indicator,
        num_tests,
        tests_indicator,
        test_time,
        case
            when cbt_phase < cbt_phase_no 
            then"Yes"
            when cbt_phase = cbt_phase_no and phase_day <= service_num_days
            then "Yes"
            else "No"
        end as unlock_indicator
    from(
        select user_id, pt_date, week_no, cbt_phase_no, service_start_date,
        case
            when cbt_phase_no = 1 then pt_date - service_start_date
            when cbt_phase_no = 2 then pt_date - service_start_date - 14
            else null
        end as service_num_days
        from(
            select distinct user_id, week_no, pt_date, cbt_phase_no, service_start_date
            from hal_report.user_record
            where pt_date = '2022-05-29'
    
        ) a
    ) main
    left join(
        select
            user_id, course_id, course_name, course_type,
            max(learn_indicator) as learn_indicator,
            min(learn_time) as learn_time,
            count(learn_time) as num_learn_time,
            quiz_indicator,
            count(complete_time) as num_tests,
            max(tests_indicator) as tests_indicator,
            min(complete_time) as test_time
        from(
    
            select
                user_id, course_id, course_name, course_type,
                case
                    when learn_time is not null 
                    then"Yes"
                    else "No"
                end as learn_indicator,
                learn_time,
                case
                    when has_quiz = 1 
                    then "Yes"
                    else "No"
                end as quiz_indicator,
                complete_time,
                case
                    when learn_time is null then"No"
                    when has_quiz = 1 and answer_correct >=1 then "Yes"
                    when has_quiz = 0 and learn_time is not null then "Yes"
                    else "No"
                end as tests_indicator,
                update_time
            from hal_report.course_record
            where pt_date = '2022-05-29'
            order by user_id, course_id
        ) grp
        group by user_id, course_id
        
    ) sub
    on main.user_id = sub.user_id
    left join(
        select course_no, cbt_phase, phase_day from hal_business_report.course_listing
    ) sub2
    on sub.course_id = sub2.course_no

) final 
order by user_id, course_id