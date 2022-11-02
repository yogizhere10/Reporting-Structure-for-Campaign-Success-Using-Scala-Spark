import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object P1CountryType extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()

  sparkConf.set("spark.app.name", "crime")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val user_schema = StructType(List(
    StructField("user_id", IntegerType),
    StructField("user_name", StringType),
    StructField("user_email", StringType),
    StructField("user_country", StringType),
    StructField("user_state", StringType),
    StructField("user_timezone", StringType),
    StructField("user_last_activity", IntegerType),
    StructField("user_device_type", StringType)))

  import spark.implicits._

  val readDf = spark.read.format("csv")
    .option("header", "true")
    .schema(user_schema)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/user_details/part-00000-46b05944-60ca-4939-995c-3c945240dc09-c000.csv")
    .load()

  val readDf1 = readDf.groupBy("user_country", "user_state", "user_timezone").count()

  val newDf = readDf1.withColumn("user", concat(col("user_country"), lit('_'), col("user_state")))

  val finalDf = newDf.selectExpr("user", "user_country", "user_state", "user_timezone")

  finalDf.show()
  
  val finalDf2 = finalDf.repartition(1)

  finalDf2.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("maxRecordsPerFile", 20000)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/countryType")
    .save()

  readDf.createOrReplaceTempView("readtable")

  val deviceDf = spark.sql("""select row_number() over(order by user_device_type) as device_key,
    user_device_type as device_type
    from readtable group by user_device_type""")
    
  val deviceDf2 = deviceDf.repartition(1)
  
    
  deviceDf2.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/deviceType")
    .save()

  val readDf2 = readDf.groupBy("user_id", "user_name", "user_email").count()

  val userDf = readDf2.withColumn("user_details_key", concat(col("user_id"), lit('_'), col("user_name")))

  val userDf2 = userDf.selectExpr("user_details_key", "user_id", "user_name", "user_email")
  
  val userDf3 = userDf2.repartition(1)

  userDf3.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/userType")
    .save()
    
  val joinType = "inner"
  
  val joinedDf = (readDf.join(finalDf, readDf.col("user_state")===finalDf.col("user_state"), "inner")
      .join(deviceDf, readDf.col("user_device_type")=== deviceDf.col("device_type"),"inner")
      .join(userDf2, readDf.col("user_name")===userDf2.col("user_name"), "inner").drop(finalDf.col("user_state"))
      .drop(deviceDf.col("device_type")).drop(userDf2.col("user_name")).drop(userDf2.col("user_id")).drop(finalDf.col("user_country"))
      .drop(userDf2.col("user_email")).drop(finalDf.col("user_timezone")))
      
  joinedDf.show(false)
  
  val joinedDf2 = joinedDf.repartition(1)
  
  joinedDf2.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/Yogesh/OneDrive/Desktop/Trendy Tech Data/Big data project/dataset/Joined1")
    .save()

}