select 
    pt_date,
    user_id,
    timing,
    case 
        when custom_image is null then "database"
        else "photo"
    end as record_manner,
    record_carbohydrate,
    case 
        when
            record_carbohydrate between recommend_carbohydrate -7.5 and recommend_carbohydrate +7.5
        then "Yes"
        else "No"
    end as carb_within_recommended,
    origin_update_time as update_time,
    scenario
from (
    select pt_date, user_id, record_date, timing, 
        max(custom_image) as custom_image,
        max(scenario) as scenario,
        sum(record_carbohydrate) as record_carbohydrate, 
        max(recommend_carbohydrate) as recommend_carbohydrate, 
        max(origin_update_time) as origin_update_time
    from hal_report_diet_daily_record
    where pt_date = '2022-05-29' and record_date between '2022-05-23' and '2022-05-29'
    group by user_id, record_date, timing
    order by user_id, record_date

) main