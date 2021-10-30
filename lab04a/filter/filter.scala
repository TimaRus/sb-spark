import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.functions._ 
 
object filter { 
  def main(args: Array[String]): Unit = { 
    val spark = SparkSession 
      .builder() 
      .appName("lab04a") 
      .getOrCreate() 
 
    import spark.implicits._ 
 
    val ofs = spark.conf.get("spark.filter.offset") 
 
    val topic = spark.conf.get("spark.filter.topic_name") 
    val offset = if(ofs != "earliest") s"""{"$topic":{"0":$ofs}}""" else "earliest" 
    val dir2 = spark.conf.get("spark.filter.output_dir_prefix") 
    val dir = file:///user/timofey.melnikov/visits2

visits2
 
 
    val kafkaParams = Map( 
      "kafka.bootstrap.servers" -> "spark-master-1:6667", 
      "subscribe" -> "lab04_input_data" 
      //, "startingOffsets" -> """ { "lab04_input_data": { "0": 2 } } """ 
      //, "endingOffsets" -> """ { "lab04_input_data": { "0": 6 } }  """ 
    ) 
 
    val df = spark 
      .read 
      .format("kafka") 
      .options(kafkaParams) 
      .option("startingOffsets", offset) 
      .load 
 
    val jsonString = df.select('value.cast("string")).as[String] 
 
    val parsed = spark 
      .read 
      .json(jsonString) 
      .select( 
        'category, 
        'event_type, 
        'item_id, 
        'item_price, 
        'timestamp, 
        'uid, 
        date_format(('timestamp / 1000).cast("timestamp"), "yyyyMMdd").alias("date"), 
        date_format(('timestamp / 1000).cast("timestamp"), "yyyyMMdd").alias("p_date") 
      ) 
 
 
    val dfView = parsed.filter('event_type === "view") 
    val dfBuy = parsed.filter('event_type === "buy") 
 
    dfView.write.format("json").mode("overwrite").partitionBy("p_date").save(dir + "/view") 
    dfBuy.write.format("json").mode("overwrite").partitionBy("p_date").save(dir + "/buy") 
 
    spark.stop() 
  } 
}