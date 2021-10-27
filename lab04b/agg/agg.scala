import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Try
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame

object agg {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("lab04b")
      .getOrCreate()

    import spark.implicits._

    val ofs = spark.conf.get("spark.filter.offset")
    val topic = spark.conf.get("spark.agg.topic_name")
    val offset = if(ofs != "earliest") s"""{"$topic":{"0":$ofs}}""" else "earliest"
    val dir = spark.conf.get("spark.agg.output_dir_prefix")

    def createConsoleSink(df: DataFrame) = {
      df
        .writeStream
        .outputMode("update")
        //.format("console")
        .format("kafka")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        //.option("checkpointLocation, s"tmp/chk/$chkName")
        .option("topic", "timofey_melnikov_lab04b_out")
        .option("truncate", "false")
        .option("numRows", "10")
    }

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "timofey_melnikov",
      "startingOffsets" -> """earliest""",
      "maxOffsetsPerTrigger" -> "200"
    )

    val sdf = spark.readStream.format("kafka").options(kafkaParams).load

    val parsedSdf = sdf.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("event_type",StringType)
      .add("category",StringType)
      .add("item_id",StringType)
      .add("item_price",IntegerType)
      .add("uid",StringType)
      .add("timestamp",StringType)

    val cleanDF = parsedSdf.select(from_json(col("value"), schema).as("data")).select("data.*")

    val aggDF = cleanDF
      .withColumn("date", to_timestamp(col("timestamp")/1000))
      .withColumn("start_date", (('timestamp / 1000 / 3600).cast("long") * 3600).cast("timestamp"))
      .withColumn("end_date", (('timestamp / 1000 / 3600).cast("long") * 3600 + 3600).cast("timestamp"))

      .withColumn("start_ts", ('timestamp / 1000 / 3600).cast("long") * 3600)
      .withColumn("end_ts", ('timestamp / 1000 / 3600).cast("long") * 3600 + 3600)
      .withColumn("uidV", when($"uid".isNull, 0).otherwise(1))
      .withWatermark("date", "1 hours")
      .groupBy(window(col("start_date"), "1 hours"), 'start_ts, 'end_ts)
      //count()
      .agg(
        sum(when(col("event_type") === "buy", col("item_price")).otherwise(0)).alias("revenue"),
        sum(when(col("event_type") === "buy" || col("event_type") === "view", col("uidV")).otherwise(0)).alias("visitors"),
        sum(when(col("event_type") === "buy", lit(1)).otherwise(0)).alias("purchases"),
        (sum(when(col("event_type") === "buy", col("item_price")).otherwise(0)) /
          sum(when(col("event_type") === "buy", lit(1)).otherwise(0))).alias("aov")
      )

    val sink = createConsoleSink(aggDF.select('start_ts, 'end_ts, 'revenue, 'visitors, 'purchases, 'aov))

    val sq = sink.start

  }
}