package com.omg.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Matchers, GivenWhenThen, FlatSpec}

/**
 * Created by robertsanders on 11/9/16.
 */
class MainTest extends FlatSpec with GivenWhenThen with Matchers with BeforeAndAfterAll {

  private val master = "local[2]"
  private val appName = this.getClass.getSimpleName

  private var _sc: SparkContext = _
  private var _sqlContext: SQLContext = _

  def sc = _sc
  def sqlContext = _sqlContext

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sc = new SparkContext(conf)
    _sqlContext = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }

    super.afterAll()
  }

  "Test" should "Test" in {
    //Main.{your_function}
    //assert(true == true)
  }

}
