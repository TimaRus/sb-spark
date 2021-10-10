import java.net.{URL}
import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

object data_mart {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("lab03")
      .config("spark.cassandra.connection.host", "10.0.0.5")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.cassandra.output.consistency.level", "ANY")
      .config("spark.cassandra.input.consistency.level", "ONE")
      .getOrCreate()

    import spark.implicits._

    val options = Map(
      "table" -> "clients",
      "keyspace" -> "labdata"
    )

    //casandra
    val client = spark.read.format("org.apache.spark.sql.cassandra").options(options).load()
      .select('uid, 'gender, when('age >= 18 && 'age <= 24, "18-24")
        .when('age >= 25 && 'age <= 34, "25-34")
        .when('age >= 35 && 'age <= 44, "35-44")
        .when('age >= 45 && 'age <= 54, "45-54")
        .when('age >= 55, ">=55").alias("age_cat")
      )

    val options2 = Map(
      "es.nodes" -> "10.0.0.5:9200",
      "es.net.http.auth.user" -> "timofey.melnikov",
      "es.net.http.auth.pass" -> "D5QMspzo",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true"
    )

    //elastic
    val df = spark.read.format("es").options(options2).load("visits")

    val df2 = df.filter('uid.isNotNull)
      .select('uid, concat(lit("shop_"), lower(regexp_replace('category, "-", "_"))).alias("category"))
      .groupBy('uid, 'category)
      .agg(count("*").alias("cnt_visits"))
      .groupBy('uid).pivot("category").agg(sum("cnt_visits"))
      .na.fill(0)

    val clients_shop = client.join(df2, "uid" :: Nil, "left")
      .select(client("uid"),
        'gender,
        'age_cat,
        'shop_cameras,
        'shop_clothing,
        'shop_computers,
        'shop_cosmetics,
        'shop_entertainment_equipment,
        'shop_everyday_jewelry,
        'shop_house_repairs_paint_tools,
        'shop_household_appliances,
        'shop_household_furniture,
        'shop_kitchen_appliances,
        'shop_kitchen_utensils,
        'shop_luggage,
        'shop_mobile_phones,
        'shop_shoes,
        'shop_sports_equipment,
        'shop_toys
      ).na.fill(0)

    //hdfs
    val raw_logs = spark.read.json("/labs/laba03/weblogs.json")
      .select($"uid", explode($"visits"))
      .select($"uid", $"col.url")

    val urls = udf { (url: String) => Try(new URL(url).getHost).toOption }

    val logs = raw_logs.filter(col("url").isNotNull)
      .withColumn("urlClean", urls(col("url")))
      .filter(col("urlClean").isNotNull)
      .withColumn("url_domain", regexp_replace(col("urlClean"), "^www\\.", ""))
      .select('uid, 'url_domain)

    //postgresql
    val jdbcUrl = "jdbc:postgresql://10.0.0.5:5432/labdata?user=timofey_melnikov&password=D5QMspzo"

    val dfjson = spark
      .read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "domain_cats")
      .load()

    val uid_cust = logs.join(dfjson, logs("url_domain") === dfjson("domain"), "inner")
      .select('uid, concat(lit("web_"), 'category).alias("category"))

    val df3 = uid_cust.filter('uid.isNotNull)
      .groupBy('uid, 'category)
      .agg(count("*").alias("cnt_visits"))
      .groupBy('uid).pivot("category").agg(sum("cnt_visits"))
      .na.fill(0)

    val clients = clients_shop.join(df3, "uid" :: Nil, "left")
      .select(clients_shop("uid"),
        'gender,
        'age_cat,
        'shop_cameras
      ).na.fill(0)

    clients.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.5:5432/timofey_melnikov")
      .option("dbtable", "clients3")
      .option("user", "timofey_melnikov")
      .option("password", "D5QMspzo")
      .option("driver", "org.postgresql.Driver")
      .mode("overwrite")
      .save()

    spark.stop()
  }
}