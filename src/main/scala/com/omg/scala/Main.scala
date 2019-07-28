package com.omg.scala

import java.io.File

import com.omg.scala.dao.ElasticSearchDaoImpl
import com.omg.scala.utils.PropertiesUtil
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._


/**
 * Created by robertsanders on 11/9/16.
 */
object Main {

  val LOGGER: Logger = LoggerFactory.getLogger(Main.getClass.getName)

  val APP_NAME: String = "TO-BE-COMPLETED"

  def main(args: Array[String]): Unit = {

    val date = args(0)
    LOGGER.info(s"""date:$date""")

    val esNodes = PropertiesUtil.getProperty("esNodes")
    val esPort = PropertiesUtil.getProperty("esPort")
    val esUri = PropertiesUtil.getProperty("esUri").replace("${esNodes}", esNodes).replace("${esPort}", esPort)
    val indexName = PropertiesUtil.getProperty("indexName").replace("${date}", date).toLowerCase
    val indexType = PropertiesUtil.getProperty("indexType").toLowerCase
    val resource = PropertiesUtil.getProperty("resource").replace("${indexName}", indexName).replace("${indexType}", indexType)
    val resourceUrl = PropertiesUtil.getProperty("resourceUrl").replace("${esNodes}", esNodes).replace("${esPort}", esPort).replace("${resource}", resource)
    val deleteByQuery = PropertiesUtil.getProperty("deleteByQuery").replace("${date}", date)
    val hiveSql = PropertiesUtil.getProperty("hiveSql").replace("${date}", date)
    val hiveId = PropertiesUtil.getProperty("hiveId")
    LOGGER.info(
      s"""
         |esNodes:$esNodes
         |esPort:$esPort
         |esUri:$esUri
         |indexName:$indexName
         |indexType:$indexType
         |resource:$resource
         |resourceUrl:$resourceUrl
         |deleteByQuery:$deleteByQuery
         |hiveSql:$hiveSql
         |hiveId:$hiveId
         |"""
        .stripMargin)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("appName")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("es.nodes", esNodes)
      .config("es.port", esPort)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val sqlDF = sql(hiveSql).rdd
    LOGGER.info(
      s"""hiveSql count:${sqlDF.count()}
         |""".stripMargin)

    val jest = new ElasticSearchDaoImpl(esUri)

    if (!indexName.contains(date)) {
      jest.deleteDocumentByQuery(indexName, indexType, deleteByQuery)
    } else {
      jest.deleteIndex(indexName)
      jest.createIndex(indexName)
    }

    if (hiveId.equalsIgnoreCase("null")) {
      sqlDF.saveToEs(resource)

    } else {
      sqlDF.saveToEs(resource, Map("es.mapping.id" -> hiveId))
    }


    spark.stop()

  }

}
