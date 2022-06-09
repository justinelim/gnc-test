import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node hal_report.user_record
hal_reportuser_record_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="halpilot_lnd",
    table_name="hal_report_user_record",
    transformation_ctx="hal_reportuser_record_node1",
)

# Script generated for node hal_report.train_execution_record
hal_reporttrain_execution_record_node1653580036628 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="halpilot_lnd",
        table_name="hal_report_train_execution_record",
        transformation_ctx="hal_reporttrain_execution_record_node1653580036628",
    )
)

# Script generated for node hal_report.exercise_policy_daily_record
hal_reportexercise_policy_daily_record_node1653579966642 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="halpilot_lnd",
        table_name="hal_report_exercise_policy_daily_record",
        transformation_ctx="hal_reportexercise_policy_daily_record_node1653579966642",
    )
)

# Script generated for node SQL_with_variables
SqlQuery26 = """
set _pt_date = '2022-05-22'
set _plan_date_start = '2022-05-16'
set _plan_date_end = '2022-05-22'

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
    date_format(sub.update_time, "HH:mm:ss") as update_time
    
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
        where pt_date = $(_pt_date)
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
        where pt_date = $(_pt_date) and training_schema_type <> 'rest'
        and plan_date between $(_plan_date_start) and $(_plan_date_end)
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
        where pt_date = $(_pt_date)
        and date(train_clock_date) between $(_plan_date_start) and $(_plan_date_end)
    )  grped
    group by user_id, train_clock_date, training_schema_type, record_type

) sub
on main.user_id = sub.user_id 
and main.plan_date = sub.train_clock_date 
and main.exercise_type = sub.training_schema_type
order by main.user_id, plan_date
"""
SQL_with_variables_node1654152165708 = sparkSqlQuery(
    glueContext,
    query=SqlQuery26,
    mapping={
        "hal_report_train_execution_record": hal_reporttrain_execution_record_node1653580036628,
        "hal_report_user_record": hal_reportuser_record_node1,
        "hal_report_exercise_policy_daily_record": hal_reportexercise_policy_daily_record_node1653579966642,
    },
    transformation_ctx="SQL_with_variables_node1654152165708",
)

# Script generated for node Amazon S3
AmazonS3_node1654153757290 = glueContext.getSink(
    path="s3://datalake-halpilot-test/processed/exercise_policy_daily_record/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1654153757290",
)
AmazonS3_node1654153757290.setCatalogInfo(
    catalogDatabase="halpilot_tgt", catalogTableName="test_exercise_delete_later"
)
AmazonS3_node1654153757290.setFormat("json")
AmazonS3_node1654153757290.writeFrame(SQL_with_variables_node1654152165708)
job.commit()
