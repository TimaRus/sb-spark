import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession}

object features {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("lab06").getOrCreate()
    import spark.implicits._

    //Загружаем SRC веб-логов
    val web_logs = spark.read.option("encoding", "UTF-8").json("/labs/laba03/weblogs.json")

    //Подготавливаем домены
    val readyWebLogs = web_logs
      .withColumn("visits", explode('visits))
      .withColumn("host", lower(callUDF("parse_url", $"visits.url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .withColumn("timestamp", to_utc_timestamp(to_timestamp($"visits.timestamp" / 1000), "Europe/Moscow"))
      .drop("visits", "host")
      .na.drop()

    //readyWebLogs.show(10)

    //Формируем топ 1000 доменов
    val TopDomainsList =
      readyWebLogs
        .groupBy("domain")
        .count()
        .orderBy(desc("count"))
        .limit(1000)
        .drop("count")
        .orderBy(asc("domain"))

    //TopDomainsList.show(10)

    //Формируем матрицу User X Domains
    val UserDomains =
      readyWebLogs.join(TopDomainsList, Seq("domain"), "inner")
        .groupBy("uid")
        .pivot("domain")
        .agg(count("domain"))
        .na.fill(0)

    //UserDomains.show(10)

    //Создание вектора
    val vector = UserDomains.columns.drop(1).map(c => UserDomains(s"`$c`"))

    //println(vector.mkString(" "))

    //Добавляем вектор к UID
    val UserDomainsResult = UserDomains
      .withColumn("domain_features", array(vector : _*))
      .select("uid", "domain_features")

    UserDomainsResult.show(10, false)

    val PreResult = readyWebLogs
      .groupBy("uid")
      .agg(
        count(when(dayofweek('timestamp) === 1, "domain")).as("web_day_sun"),
        count(when(dayofweek('timestamp) === 2, "domain")).as("web_day_mon"),
        count(when(dayofweek('timestamp) === 3, "domain")).as("web_day_tue"),
        count(when(dayofweek('timestamp) === 4, "domain")).as("web_day_wed"),
        count(when(dayofweek('timestamp) === 5, "domain")).as("web_day_thu"),
        count(when(dayofweek('timestamp) === 6, "domain")).as("web_day_fri"),
        count(when(dayofweek('timestamp) === 7, "domain")).as("web_day_sat"),
        count(when(hour('timestamp) === 0, "domain")).as("web_hour_0"),
        count(when(hour('timestamp) === 1, "domain")).as("web_hour_1"),
        count(when(hour('timestamp) === 2, "domain")).as("web_hour_2"),
        count(when(hour('timestamp) === 3, "domain")).as("web_hour_3"),
        count(when(hour('timestamp) === 4, "domain")).as("web_hour_4"),
        count(when(hour('timestamp) === 5, "domain")).as("web_hour_5"),
        count(when(hour('timestamp) === 6, "domain")).as("web_hour_6"),
        count(when(hour('timestamp) === 7, "domain")).as("web_hour_7"),
        count(when(hour('timestamp) === 8, "domain")).as("web_hour_8"),
        count(when(hour('timestamp) === 9, "domain")).as("web_hour_9"),
        count(when(hour('timestamp) === 10, "domain")).as("web_hour_10"),
        count(when(hour('timestamp) === 11, "domain")).as("web_hour_11"),
        count(when(hour('timestamp) === 12, "domain")).as("web_hour_12"),
        count(when(hour('timestamp) === 13, "domain")).as("web_hour_13"),
        count(when(hour('timestamp) === 14, "domain")).as("web_hour_14"),
        count(when(hour('timestamp) === 15, "domain")).as("web_hour_15"),
        count(when(hour('timestamp) === 16, "domain")).as("web_hour_16"),
        count(when(hour('timestamp) === 17, "domain")).as("web_hour_17"),
        count(when(hour('timestamp) === 18, "domain")).as("web_hour_18"),
        count(when(hour('timestamp) === 19, "domain")).as("web_hour_19"),
        count(when(hour('timestamp) === 20, "domain")).as("web_hour_20"),
        count(when(hour('timestamp) === 21, "domain")).as("web_hour_21"),
        count(when(hour('timestamp) === 22, "domain")).as("web_hour_22"),
        count(when(hour('timestamp) === 23, "domain")).as("web_hour_23"),
        count(when(hour('timestamp) >= 9 && hour('timestamp) < 18, "domain")).as("web_work_hours"),
        count(when(hour('timestamp) >= 18 && hour('timestamp) < 24, "domain")).as("web_evening_hours"),
        count("domain").as("visits"))
      .withColumn("web_fraction_work_hours", 'web_work_hours / 'visits)
      .withColumn("web_fraction_evening_hours", 'web_evening_hours / 'visits)

    //PreResult.show(20)

    val user_items = spark.read.parquet("/user/timofey.melnikov/users-items/20200429").na.drop()
    val result =
      PreResult
        .drop("web_work_hours", "web_evening_hours", "visits")
        .join(UserDomainsResult, Seq("uid"), "left").na.fill(0)
        .join(user_items, Seq("uid"), "full").na.fill(0)

    //result.show(20)

    result.write.mode("overwrite").parquet("/user/timofey.melnikov/features")

  }

}