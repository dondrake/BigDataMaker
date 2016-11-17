package com.drakeconuslting.big_data_maker

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, StringType}

class ColumnsTest extends FunSuite with SharedSparkContext {
  test("StringConstant") {
    val s1 = new StringConstant("f1", "abc")
    assert("abc" === s1.getValue(1))
    assert(StructField("f1", StringType, false) == s1.getStructField)
  }
}
  
