import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.log4j.Level
import org.apache.log4j.Logger

object P1Eventdata extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()

  sparkConf.set("spark.app.name", "crime")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val schemaclick = StructType(List(
    StructField("campaign_id", LongType),
    StructField("course_campaign_name", StringType),
    StructField("user_id", IntegerType),
    StructField("user_name", StringType),
    StructField("campaign_date", StringType),
    StructField("digital_marketing_team", StringType),
    StructField("event_status", IntegerType),
    StructField("event_type", StringType),
    StructField("marketing_product", StringType),
    StructField("user_response_time", IntegerType)))

  val clickDf = spark.read.format("csv")
    .option("header", "true")
    .schema(schemaclick)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/hubspot_email_click_link_event/part-00000-d326e1da-15af-4cb2-8f5d-b8d04887d522-c000.csv")
    .load()

  clickDf.createOrReplaceTempView("tableclick")

  val finalClickDf = spark.sql("""
    select Concat(campaign_id,'_',course_campaign_name)as campaign_result_key,
    Cast((sum(event_status)/ count(event_status)*100) as Integer) as campaign_actual_click_percent,
    Sum(event_status) as campaign_actual_click
    from tableclick
    Group by campaign_id, course_campaign_name""")

  val schemaopen = StructType(List(
    StructField("campaign_id", LongType),
    StructField("course_campaign_name", StringType),
    StructField("user_id", IntegerType),
    StructField("user_name", StringType),
    StructField("campaign_date", StringType),
    StructField("digital_marketing_team", StringType),
    StructField("event_status", IntegerType),
    StructField("event_type", StringType),
    StructField("marketing_product", StringType),
    StructField("user_response_time", IntegerType)))

  val openDf = spark.read.format("csv")
    .option("header", "true")
    .schema(schemaopen)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/hubspot_email_open_event/part-00000-7de9077b-c3de-451f-a6a5-f1645801a043-c000.csv")
    .load()

  openDf.createOrReplaceTempView("tableopen")

  val finalOpenDf = spark.sql("""
    select Concat(campaign_id,'_',course_campaign_name)as campaign_result_key,
    Cast((sum(event_status)/ count(event_status)*100) as Integer) as campaign_actual_open_percent,
    Sum(event_status) as campaign_actual_open
    from tableopen
    Group by campaign_id, course_campaign_name""")

  val schemasent = StructType(List(
    StructField("campaign_id", LongType),
    StructField("course_campaign_name", StringType),
    StructField("user_id", IntegerType),
    StructField("user_name", StringType),
    StructField("campaign_date", StringType),
    StructField("digital_marketing_team", StringType),
    StructField("event_status", IntegerType),
    StructField("event_type", StringType),
    StructField("marketing_product", StringType),
    StructField("user_response_time", IntegerType)))

  val sentDf = spark.read.format("csv")
    .option("header", "true")
    .schema(schemasent)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/hubspot_email_sent_event/part-00000-d08ba870-ef87-40d3-9a83-7da9b83af171-c000.csv")
    .load()

  sentDf.createOrReplaceTempView("tablesent")

  val finalSentDf = spark.sql("""
    select Concat(campaign_id,'_',course_campaign_name)as campaign_result_key,
    Cast((sum(event_status)/ count(event_status)*100) as Integer) as campaign_actual_sent_percent,
    Sum(event_status) as campaign_actual_sent
    from tablesent
    Group by campaign_id, course_campaign_name""")

  val schemaunsub = StructType(List(
    StructField("campaign_id", LongType),
    StructField("course_campaign_name", StringType),
    StructField("user_id", IntegerType),
    StructField("user_name", StringType),
    StructField("campaign_date", StringType),
    StructField("digital_marketing_team", StringType),
    StructField("event_status", IntegerType),
    StructField("event_type", StringType),
    StructField("marketing_product", StringType),
    StructField("user_response_time", IntegerType)))

  val unsubDf = spark.read.format("csv")
    .option("header", "true")
    .schema(schemaunsub)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/hubspot_email_unsubscribe_event/part-00000-0d94ad8c-a780-4f8a-b7e1-7be0cce3ff18-c000.csv")
    .load()

  unsubDf.createOrReplaceTempView("tableunsub")

  val finalUnsubDf = spark.sql("""
    select Concat(campaign_id,'_',course_campaign_name)as campaign_result_key,
    Cast((sum(event_status)/ count(event_status)*100) as Integer) as campaign_actual_unsubscribe_percent,
    Sum(event_status) as campaign_actual_unsubscribe
    from tableunsub
    Group by campaign_id, course_campaign_name""")

  val schemacampaign = StructType(List(
    StructField("campaign_id", LongType),
    StructField("course_campaign_name", StringType),
    StructField("campaign_agenda", StringType),
    StructField("campaign_category", StringType),
    StructField("campaign_agenda_sent", IntegerType),
    StructField("campaign_agenda_open", IntegerType),
    StructField("campaign_agenda_click", IntegerType),
    StructField("campaign_agenda_unsubscribe", IntegerType),
    StructField("digital_marketing_team", StringType),
    StructField("course_campaign_start_date", StringType),
    StructField("course_campaign_end_date", StringType),
    StructField("marketing_product", StringType)))

  val df1 = spark.read.format("csv")
    .option("header", "true")
    .schema(schemacampaign)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/campaign_details/part-00000-c1d16a54-9a4f-4f4d-9401-60a6b1ef7983-c000.csv")
    .load()

  val df2 = df1.withColumn("campaign_result_key", concat(col("campaign_id"), lit('_'), col("course_campaign_name")))

  val unsubJoined = df2.join(finalUnsubDf, df2.col("campaign_result_key") === finalUnsubDf.col("campaign_result_key"), "inner").drop(finalUnsubDf.col("campaign_result_key"))

  unsubJoined.createOrReplaceTempView("mytable1")

  val finalUnsub = spark.sql("""
    select *, 
    IF(campaign_actual_unsubscribe_percent < campaign_agenda_unsubscribe, true, false) as campaign_unsubscribe_positive
    from mytable1""")

  val clickJoined = df2.join(finalClickDf, df2.col("campaign_result_key") === finalClickDf.col("campaign_result_key"), "inner").drop(finalClickDf.col("campaign_result_key"))

  clickJoined.createOrReplaceTempView("mytable2")

  val finalClick = spark.sql("""
    select *, 
    IF(campaign_actual_click_percent >= campaign_agenda_click, true, false) as campaign_click_positive
    from mytable2""")

  val openJoined = df2.join(finalOpenDf, df2.col("campaign_result_key") === finalOpenDf.col("campaign_result_key"), "inner").drop(finalOpenDf.col("campaign_result_key"))

  openJoined.createOrReplaceTempView("mytable3")

  val finalOpen = spark.sql("""
    select *, 
    IF(campaign_actual_open_percent >= campaign_agenda_open, true, false) as campaign_open_positive
    from mytable3""")

  val sentJoined = df2.join(finalSentDf, df2.col("campaign_result_key") === finalSentDf.col("campaign_result_key"), "inner").drop(finalSentDf.col("campaign_result_key"))

  sentJoined.createOrReplaceTempView("mytable4")

  val finalSent = spark.sql("""
    select *, 
    IF(campaign_actual_sent_percent >= campaign_agenda_sent, true, false) as campaign_sent_positive
    from mytable4""")

  val finalJoined1 = finalSent.join(finalOpen, finalSent.col("campaign_result_key") === finalOpen.col("campaign_result_key"), "inner").
    drop(finalSent.col("campaign_result_key")).drop(finalSent.col("campaign_id")).drop(finalSent.col("course_campaign_name")).
    drop(finalSent.col("campaign_agenda")).drop(finalSent.col("campaign_category")).drop(finalSent.col("campaign_agenda_sent")).
    drop(finalSent.col("campaign_agenda_open")).drop(finalSent.col("campaign_agenda_click")).drop(finalSent.col("campaign_agenda_unsubscribe")).
    drop(finalSent.col("digital_marketing_team")).drop(finalSent.col("course_campaign_start_date")).drop(finalSent.col("course_campaign_end_date")).
    drop(finalSent.col("marketing_product"))

  finalJoined1.show()

  val finalJoined2 = finalClick.join(finalUnsub, finalClick.col("campaign_result_key") === finalUnsub.col("campaign_result_key"), "inner").
    drop(finalClick.col("campaign_result_key")).drop(finalClick.col("campaign_id")).drop(finalClick.col("course_campaign_name")).
    drop(finalClick.col("campaign_agenda")).drop(finalClick.col("campaign_category")).drop(finalClick.col("campaign_agenda_sent")).
    drop(finalClick.col("campaign_agenda_open")).drop(finalClick.col("campaign_agenda_click")).drop(finalClick.col("campaign_agenda_unsubscribe")).
    drop(finalClick.col("digital_marketing_team")).drop(finalClick.col("course_campaign_start_date")).drop(finalClick.col("course_campaign_end_date")).
    drop(finalClick.col("marketing_product"))

  val finalResult = finalJoined1.join(finalJoined2, finalJoined1.col("campaign_result_key") === finalJoined2.col("campaign_result_key"), "inner").
    drop(finalJoined1.col("campaign_result_key")).drop(finalJoined1.col("campaign_id")).drop(finalJoined1.col("course_campaign_name")).
    drop(finalJoined1.col("campaign_agenda")).drop(finalJoined1.col("campaign_category")).drop(finalJoined1.col("campaign_agenda_sent")).
    drop(finalJoined1.col("campaign_agenda_open")).drop(finalJoined1.col("campaign_agenda_click")).drop(finalJoined1.col("campaign_agenda_unsubscribe")).
    drop(finalJoined1.col("digital_marketing_team")).drop(finalJoined1.col("course_campaign_start_date")).drop(finalJoined1.col("course_campaign_end_date")).
    drop(finalJoined1.col("marketing_product"))
    
  val finalResult1 = finalResult.repartition(1)
  
  finalResult.show()

  finalResult1.write
    .format("csv")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/finalReport1")
    .save()

}