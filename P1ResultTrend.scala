import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.log4j.Level
import org.apache.log4j.Logger

object P1ResultTrend extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()

  sparkConf.set("spark.app.name", "crime")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val resultDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/finalReport1/part-00000-94114aea-a465-4f0d-a54a-3f26ae7d4df6-c000.csv")
    .load()

  val eventOpenDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/hubspot_email_open_event/part-00000-7de9077b-c3de-451f-a6a5-f1645801a043-c000.csv")
    .load()

  val userDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/user_details/part-00000-46b05944-60ca-4939-995c-3c945240dc09-c000.csv")
    .load()

  eventOpenDf.createOrReplaceTempView("opentable")
  resultDf.createOrReplaceTempView("resulttable")
  userDf.createOrReplaceTempView("usertable")

  val interDf = spark.sql("""
    select a.campaign_id, u.user_device_type, count(user_device_type) as share
    from resulttable a JOIN opentable b
    ON a.campaign_id = b.campaign_id
    JOIN usertable u
    ON b.user_id = u.user_id
    WHERE a.campaign_open_positive = true
    Group By a.campaign_id, u.user_device_type""")

  interDf.createOrReplaceTempView("intertable")

  val res1Df = spark.sql("""select * from intertable 
    pivot(sum(share) for user_device_type in ('tablet','mobile','laptop'))""")

  val inter2Df = spark.sql("""
    select a.campaign_id, 
    replace(u.user_state,' ','_') as user_state,
    count(user_state) as state_share
    from resulttable a JOIN opentable b
    ON a.campaign_id = b.campaign_id
    JOIN usertable u
    ON b.user_id = u.user_id
    WHERE a.campaign_open_positive = true
    Group By a.campaign_id, u.user_state""")

  inter2Df.createOrReplaceTempView("inter2table")

  val res2Df = spark.sql("""select * from inter2table
    pivot (sum(state_share) for  user_state in ('Illinois','California','Georgia','Arizona','Florida','West_Virginia',
    'Texas','Massachusetts','Wisconsin','District_of_Columbia','Kentucky','Missouri','Mississippi','Alabama',
    'Ohio','Michigan','Oklahoma','Virginia','New_York','Washington','Colorado','Pennsylvania','Indiana',
    'North_Carolina','Minnesota','Tennessee','New_Jersey','Idaho','Nevada','Louisiana','New_Mexico','Maryland',
    'South_Carolina','Oregon','Connecticut','Alaska','Iowa','Montana','Rhode_Island','Utah','Arkansas','Kansas','Nebraska',
    'Hawaii','North_Dakota','New_Hampshire','Maine','South_Dakota'))""")

  res2Df.createOrReplaceTempView("state_metric")
  res1Df.createOrReplaceTempView("device_metric")

  val finalResDf = spark.sql("""
    select a.course_campaign_name, a.campaign_agenda,
    a.campaign_category, a.digital_marketing_team,
    a.marketing_product,
    b.mobile, b.tablet, b.laptop, c.*
    from resulttable a JOIN device_metric b
    ON a.campaign_id = b.campaign_id
    JOIN state_metric c
    ON a.campaign_id = c.campaign_id""")

  finalResDf.show(false)
  
  val finalResult = finalResDf.repartition(1)
  
  finalResult.write
    .format("csv")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/finalRes")
    .save()

}