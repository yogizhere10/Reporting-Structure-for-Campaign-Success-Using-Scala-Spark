import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.log4j.Level
import org.apache.log4j.Logger

object P1CampaignDim extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()

  sparkConf.set("spark.app.name", "crime")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

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

  def splitAndReturn(name: String) = {
    val fields = name.split("_")
    fields(1)
  }

  spark.udf.register("getString", splitAndReturn(_: String): String)

  df1.createOrReplaceTempView("table")

  val marketingDf = spark.sql("""select row_number() over(order by dig_marketing_team) as marketing_key,
    UPPER(dig_marketing_team) as digital_marketing_team,
    UPPER(getString(dig_marketing_team)) as marketing_place
    from
    (select distinct(digital_marketing_team) as dig_marketing_team from table
    order by dig_marketing_team asc)""")

  marketingDf.show(false)

}