import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object filter extends App {

  val spark = SparkSession
    .builder()
    .appName("lab04a")
    .getOrCreate()

  import spark.implicits._

  //val ofs = spark.conf.get("spark.filter.offset")
  val topic = spark.conf.get("spark.filter.topic_name")
  //val offset = if(ofs != "earliest") s"""{"$topic":{"0":$ofs}}""" else "earliest"
  val dir = spark.conf.get("spark.filter.output_dir_prefix")

val offset = spark.conf.get("spark.filter.offset")
val offset = if (offset != "earliest") {
  offset = s"""{"$topic":{"0":$offset}}"""
}

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> "lab04_input_data"
  )

  val df = spark.read.format("kafka").options(kafkaParams).option("startingOffsets", offset).load

  val jsonString = df.select('value.cast("string")).as[String]

  val parsed = spark.read.json(jsonString)

  def rountTimestamp(timestampCol: Column): Column = {
    date_format(((timestampCol / 1000).cast("timestamp")),"yyyyMMdd")
  }

  val view = parsed
    .filter("event_type = 'view'")
    .withColumn("date", rountTimestamp('timestamp))
    .withColumn("datePart", rountTimestamp('timestamp))
    .orderBy("date")

  val buy = parsed
    .filter("event_type = 'buy'")
    .withColumn("date", rountTimestamp('timestamp))
    .withColumn("datePart", rountTimestamp('timestamp))
    .orderBy("date")

  view.write.format("json").mode("overwrite").partitionBy("p_date").save(dir + "/view")
  buy.write.format("json").mode("overwrite").partitionBy("p_date").save(dir + "/buy")

  spark.stop()
}