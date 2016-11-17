package com.drakeconsulting.big_data_maker

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StringType, LongType, DoubleType}

class ColumnsTest extends FunSuite with SharedSparkContext {
  test("test StringConstant") {
    val s1 = new StringConstant("f1", "abc")
    assert("abc" === s1.getValue(1))
    assert(StructField("f1", StringType, false) == s1.getStructField)
  }

  test("test RandomLong") {
    val s1 = new RandomLong("f1", 666666L)
    assert(s1.getValue(1) >= 0)
    assert(s1.getValue(1) <= 666666L)
    assert(StructField("f1", LongType, false) == s1.getStructField)
  }

  test("test RandomDouble") {
    val s1 = new RandomDouble("f1", 666666.00)
    assert(s1.getValue(1) >= 0)
    assert(s1.getValue(1) <= 666666.00)
    assert(StructField("f1", DoubleType, false) == s1.getStructField)
  }
}
  
