/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package com.drakeconsulting.big_data_maker

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import java.io.File

object SimpleApp {
  def main(args: Array[String]) {
    case class Config(outDir: File = new File("."))
    val parser = new scopt.OptionParser[Config]("Big Data Maker") {
      head("Big Data Maker", "1.0")
      opt[File]('o', "outDir").required.action( (x,c) => c.copy(outDir = x) ).text("outDir is a directory")
    }

    var outDir = ""
    parser.parse(args, Config()) match {
      case Some(config) =>
        outDir = config.outDir.toString

      case None =>
        println("Oops.")
        System.exit(1)
    }

    println("outDir=" + outDir)
    val conf = new SparkConf().setAppName("Big Data Maker Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val bigData = new BigData(sqlContext, outDir, 10, 1000)
    bigData.addColumn(new StringConstant("f1", "testing"))
    bigData.addColumn(new RandomLong("f2", 100000000000L))
    bigData.addColumn(new RandomLong("f3", 10000000000L))
    bigData.addColumn(new RandomLong("f4", 1000000000L))
    bigData.addColumn(new RandomDouble("f5", 1000000000.0))
    bigData.addColumn(new RandomDouble("f6", 100000000.0))
    bigData.addColumn(new RandomDouble("f7", 10000000.0))

    bigData.writeFile

  }
}
