package com.drakeconsulting.big_data_maker

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext

class BigDataMakerTest extends FunSuite with SharedSparkContext {
  test("first") {
    val sqlContext = new SQLContext(sc)
    val bd = new BigData(sqlContext, "/tmp/b", 5, 100)
    bd.addColumn(new StringConstant("f1", "abc"))
    bd.addColumn(new StringConstant("f2", "def"))

    val df = bd._createDataFrame
    df.show
    assert(500 === df.count)
    assert(2 === df.columns.length)
  }
}
  
