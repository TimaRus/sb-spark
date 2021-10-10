import com.typesafe.scalalogging.Logger
import org.apache.commons.cli.{CommandLine, Options, PosixParser}
import org.apache.spark.sql.functions.{broadcast, col, count, explode, expr, lit, udf, when}

import java.net.{URL, URLDecoder}
import scala.collection.immutable.HashMap
import org.postgresql.Driver
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.elasticsearch.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.DataType

import java.io.FileReader
import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties


object data_mart extends App {
  case class CsServer( host:String, port:Int)
  val labName = "lab-03-xa0c"
  @transient lazy val logger = Logger(labName)
  val props:Properties = new Properties()
  val option:Options = new Options
  var cmd:CommandLine = null
  try {
    DriverManager.registerDriver(new org.postgresql.Driver)
    option.addOption("p", "run-property", true, "run property file")
    cmd = (new PosixParser).parse(option, args)
  } catch {
    case e: Exception  => {
      logger.error("Unable to parse command-line options: " + e.getMessage)
      e.printStackTrace()
    }
  }

  // Load property
  logger.info(s"run-property=${cmd.getOptionValue("run-property")}")
  props.load( new FileReader( cmd.getOptionValue("run-property", "/data/home/timofey.melnikov/labs/lab03.property") ) )
  // HDFS
  val hdfsSourceWebClik = props.getProperty("hdfs.source.webclik")
  logger.info(s"hdfs.source=${hdfsSourceWebClik}")

  // cassandra
  val csServer = CsServer (props.getProperty("cs.host"), props.getProperty("cs.port").toInt)
  val csTClients:HashMap[String,String] = HashMap( "keyspace"->"labdata", "table"->"clients")
  logger.info(s"cs.host=${csServer.host}:${csServer.port}")

  // elastic
  val esOpts:HashMap[String,String] = HashMap(
    "es.nodes" -> props.getProperty("es.nodes")
    , "es.nodes.wan.only" -> "true"
    , "es.net.http.auth.user" -> props.getProperty("es.usename")
    , "es.net.http.auth.pass" -> props.getProperty("es.password")
  )
  logger.info(s"es.host=${esOpts("es.nodes")}")

  val esIndex = "visits"

  // postgre
  val pgOpts:HashMap[String,String] = HashMap(
    "driver" -> "org.postgresql.Driver"
    , "user" -> props.getProperty("pg.usename")
    , "password" -> props.getProperty("pg.password")
  )
  logger.info(s"es.user=${pgOpts("user")}")

  val pgSourceUrl:String = props.getProperty("pg.jdbc.url") + "/" + "labdata"
  val pgTargetUrl:String = props.getProperty("pg.jdbc.url") + "/" + pgOpts("user")

  val pgTblDomain:HashMap[String,String]  =
    HashMap ( "url" -> pgSourceUrl
      , "dbtable" -> "domain_cats"
    )
  val pgTblDataMarket:HashMap[String,String]  =
    HashMap ( "url" -> pgTargetUrl
      , "dbtable" -> "clients"
    )

  // config sparck

  @transient lazy val pgTargetDb:Connection = DriverManager.getConnection(pgTargetUrl, pgOpts("user"), pgOpts("password"))
  val spark = SparkSession
    .builder
    .appName(labName)
    .getOrCreate()

  spark.conf.set(s"spark.cassandra.connection.host", csServer.host)
  spark.conf.set(s"spark.cassandra.connection.port", csServer.port)

  // UDF
  def url_domain_2level:UserDefinedFunction = udf(( url : String)  => {
    val urloption :URL = try {
      new URL(URLDecoder.decode(url, "utf-8"))
    } catch {
      case e: Exception => null
    }

    if ( urloption != null ) {
      val host :String = urloption.getHost match {
        case x: String if x.startsWith("www.") => x.substring(4)
        case x: String => x
      }
      host  //.split('.').takeRight(2).mkString(".")
      // пробывать домены с разными условиями
    } else {
      null
    }

  })
  private def targetExecureDDL( sql : String) = {
    val pgTargetStatement:Statement = pgTargetDb.createStatement()
    pgTargetStatement.execute( sql )
    pgTargetStatement.close()

  }

  // postgre target
  lazy val pgDataMarket =  spark
    .read
    .format("jdbc")
    .options(pgOpts)
    .options(pgTblDataMarket)
    .load()


  lazy val csClients =  spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options( csTClients )
    .load()
    .where(col("age")>=18)
    .withColumn("age_cat"
      ,  when( col("age").between(18,24),"18-24")
        .when( col("age").between(25,34),"25-34")
        .when( col("age").between(35,44),"35-44")
        .when( col("age").between(45,44),"45-54")
        .otherwise(">=55")
    )
    .drop("age")

  lazy val esShops =  spark
    .read.format("org.elasticsearch.spark.sql")
    .options( esOpts)
    .load( esIndex )
    //.where(col("event_type")==="buy")
    .withColumn("shop_name"
      , functions.concat(
        lit("shop_")
        , functions.regexp_replace(
          functions.lower( col("category") )
          , "[ -]"
          , "_"
        )
      )
    )
    .select("uid", "shop_name")


  lazy val hdfsWebClik =  spark
    .read.format("json")
    .load( hdfsSourceWebClik )
    .where(col("uid").isNotNull)
    .select(col("uid"), explode(col("visits")).as("obj"))
    .withColumn("domain", url_domain_2level(col("obj.url")))
    .select(col("uid"), col("domain"), col("obj.url").as("url"))

  lazy val pgWebCategory =  spark
    .read.format("jdbc")
    .options(pgOpts)
    .options(pgTblDomain)
    .load()
    .withColumn("cat_name"
      , functions.concat(
        lit("web_")
        , functions.regexp_replace(
          functions.lower( col("category") )
          , "[ -]"
          , "_"
        )
      )
    )
    .select("domain", "cat_name")


  lazy val web_result =
    hdfsWebClik
      .join(broadcast(pgWebCategory), Seq("domain"), "inner")
      .select("uid", "url", "domain", "cat_name")
      .groupBy(col("uid"))
      .pivot("cat_name")
      .agg(count("uid"))



  lazy  val shop_result =
    esShops
      .groupBy(col("uid"))
      .pivot("shop_name")
      .agg(count("uid"))

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  lazy val result = csClients
    .join(shop_result,Seq("uid"), "left")
    .join(web_result, Seq("uid"), "left")
    .na.fill(0)
    .repartition(10)

  targetExecureDDL("DROP TABLE IF EXISTS clients")

  result
    .write
    .format("jdbc")
    .options(pgOpts)
    .options(pgTblDataMarket)
    .mode(SaveMode.Overwrite)
    .save()

  targetExecureDDL("GRANT SELECT ON TABLE clients TO labchecker2")
  // Finall
  spark.close()
}