import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.functions._ 
import org.apache.hadoop.fs.{FileSystem, Path} 
 
object users_items { 
  def main(args: Array[String]): Unit = { 
 
    val spark = SparkSession 
      .builder() 
      .appName("lab05") 
      .config("spark.sql.session.timeZone", "UTC") 
      .getOrCreate() 
 
    import spark.implicits._ 
 
    val inp_dir = spark.conf.get("spark.users_items.input_dir")
    val out_dir = spark.conf.get("spark.users_items.output_dir")
    val upd = spark.conf.get("spark.users_items.update").toInt    // 
 
    val dfView = spark.read.json(inp_dir + "/view") 
      .filter('uid.isNotNull) 
      .select( 
        date_format(('timestamp / 1000).cast("timestamp"), "yyyyMMdd").cast("int").alias("date"), 
        concat('event_type, lit("_"), lower(regexp_replace('item_id, "-", "_"))).alias("item"), 
        'uid 
      ) 
 
    val dfBuy = spark.read.json(inp_dir + "/buy") 
      .filter('uid.isNotNull) 
      .select( 
        date_format(('timestamp / 1000).cast("timestamp"), "yyyyMMdd").cast("int").alias("date"), 
        concat('event_type, lit("_"), lower(regexp_replace('item_id, "-", "_"))).alias("item"), 
        'uid 
      ) 
 
    val dt_inp = dfView 
      .union(dfBuy) 
      .cache 
      .select(max('date)) 
      .take(1)(0)(0) 
 
    val df = dfView 
      .union(dfBuy) 
      .groupBy('uid, 'item) 
      .agg(count("*").alias("cnt")) 
      .groupBy('uid).pivot("item").agg(sum("cnt")) 
      .na.fill(0) 
 
    if (upd == 0) { 
      df.write.format("parquet").mode("overwrite").save(out_dir + "/" + dt_inp.toString) 
    } else { 
      /*val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration) 
      val status = fs.listStatus(new Path(out_dir)) 
      val dt_old = status.map(x=> x.getPath.getName.toInt).max*/ 
 
      val path = new Path(out_dir) 
      val dt_old = path.getFileSystem(spark.sparkContext.hadoopConfiguration).listStatus(path).map(x=> x.getPath.getName.toInt).max 
 
      val dt_max = List(dt_inp.toString.toInt, dt_old).max 
 
      val df_old = spark.read.schema(df.schema).json(out_dir + "/*") 
 
      df_old.union(df).write.format("parquet").mode("overwrite").save(out_dir + "/" + "20200430/") 
    } 
 
    spark.stop 
 
  } 
} 
