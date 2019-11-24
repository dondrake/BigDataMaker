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

object SimpleApp {
  def main(args: Array[String]) {

    case class Config(outDir: String = ".", numPartitions: Integer = 0, numRows: Integer = 0)

    val parser = new scopt.OptionParser[Config]("Big Data Maker") {
      head("Big Data Maker", "1.0")
      opt[String]('o', "outDir").required.action( (x,c) => c.copy(outDir = x) ).text("outDir is a directory")
      opt[Int]('p', "numPartitions").action( (x,c) => c.copy(numPartitions = x) ).text("# of partitions")
      opt[Int]('r', "numRows").action( (x,c) => c.copy(numRows = x) ).text("# of rows/partition")
    }

    var outDir = ""
    var numPartitions: Integer = 0
    var numRows: Integer = 0

    parser.parse(args, Config()) match {
      case Some(config) =>
        outDir = config.outDir
        numPartitions = config.numPartitions
        numRows = config.numRows

      case None =>
        println("Oops.")
        System.exit(1)
    }

    if (numPartitions == 0) {
      numPartitions = 10
    }
    if (numRows == 0) {
      numRows = 1000
    }

    println("outDir=" + outDir)
    println("numPartitions=" + numPartitions)
    println("numRows=" + numRows)

    val conf = new SparkConf().setAppName("Big Data Maker Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val states = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MT NE NV NH NJ NM NY NC ND OH OK OR MD MA MI MN MS MO PA RI SC SD TN TX UT VT VA WA WV WI WY".split(" ").toList
    val randomStrings = "NmvUuFRItfdzErKMWlajYFrVSkyCEsnjQ7cHPEd6XoHoqOVquFwT2TvkfULEJ5FnY60bakIAH8FIK6gKiabuPcEHNJteAF7Iio3tVIeAXUVUyfTt8Bsi6Ux7gFAwRnzCPNIxD90z1Wjy9L2m19DC2XsCBGaXVxYm UFHRzHiu4988EfGfK3ZIdKnbZSjMGWSresgBoHDFVmVNG2dt3Wdhkdq53hV9nFgPofwEoJSGpUAx0ZtbmD5qteTRX1Tho9biVfKv6BsWxH5M9IakvqmcSZ2Kktubl2AysVvDEKyuPMbWwXUSvr8C9iOVbe9D6gb4 TF29vVNdoVFB1otXVDtyccO9Ou3OKlyBacF1NYDihMuptFw9L4BX1gmtpnGaT6Uj716RiWEpFBYewfxjch4IahYSEdY5cVso4gtChOpFAWpMT9Bi51iubo0GVaGDI0Y6a8PMvlqUTSqVDD11KccNlyRaYb2aX9vV veEV9SnSfgdvsSlunTPA2Ugj0A8M8P6MdgCIOCP9bG68eP1h0pZfcOyVWWVROOyNoZb187sPPMrQVPOUoHOz8GasEhnwCWv1QGBmUZCWNFoOwefpwNYv9dA9AZJgpuIntK8Ce6HTIZbenC73yfUNOhSLrZaJqqzD NQn42h8fvlHvjnexTsNdVFF1VqqX8ebWzEwI6HA17o51DqvscEzNWW6BmVsmDHkLDnJUgXMz9Gr3FO9vfvX4jNC2gVMpRElQKdpZ1O4TBKQMAoA7ZhvJIBSom0MApifhxFj0Wp7huvqIXtta7I7FiW3a4XMKItds 2PCNCDwGpAwqZzhyrFTBkUUvlTMidLyHl649B1UCS7hbudNWYWUflmXR3ft6tDuvTBQVlZXva3E4kXZp5sIphkvPJ0kuD6kaxaHl8kqqQfw5u2P5RkLTJCpUgIF8I598VNckw7YkHtIiJnIc5eNghIqn9PSFK5va QQ8Gx6ohLtrQfP6ONoJmhOv5TNBPq8TlU6ZaqEFrQlxi6scsOLwJpUAYeU6LTbf7Ivu7rU9j33k3V4aQmXJbTnY1oOEOAnbWzxvzOc43dFDasP7l7NIZBHo9vwQbscR9qfWM9pT0kP0XcfcZ13HvlqweN5oL8EVt whdUQdYcL9V3Odd9glRnHPJSbah0xz4jT0b1dQoc6nzduQQaYM65AsFZby2ZAzoAIQ3EDnjUzHu8PPhQMfZ6Q4Xtq95aLOwakJvdrz4IQYC15DfvnV1P97e1JHh3M47QNaTuBUoytGX6istdWLm8ciTRwz6OyZMH ItvEdo4XTsnlpEHb9GuZFqcH3JPILQniGPfDCiZTg0xWjA6r7Z69zwOtfUyklyefI1qvDF46tnb1bHDe9eYMwTT0J9C3t72pztOSpHfWflSMq6UBsrH2r0hRE2rgsNQzYkhD1hW5h7hqdiHZB2HNAyuWQlXTSHwF lDXClVx6LPHCOMDfuFe2H8LxKzwvvJ3BKFBmz2fT7HtKTYmo3SZpul7ElYKxFjvLVERBgdggesK0qXRO06UhWN5XC5JPqvLfGl5OpgDMpIzCzxOH7zK71YFosnfzjCeXT1ITpj5YbwWXD2OxrN5ZZ8J7h8x55Wx2 fN11BO8ZbOjUv4yNkVUAQCw5m8hdeYQ4Fy24uB0YUGtTZBOmySlqTKSqX5cAgurElaBgbJ6KPzneJDhHWFDSk55MY6Zm2jrD00e2dkXDqJmoOoPJ9P4FyveiW5mAfvrISPBTMySPLNievcPwmhYhwIDWotvzC8p0 88cVMquwNRqrHVeVCXxrNyXOFK0ghvgayezBMDaAb3KY7I6IFSZEFIkgRtGNIojZvS4JDeMjqdvjny0MxaSKyVCKoLhL1bV56RE8zcJ4KyO9bBq3E8PXZqok3uFHFnESeVxQFH9v13CmX6kbfUVcYaUcRuuHSkac gFKzTrU5JM4UgQuBzCxwX3w84RFQYUHzc32klDZQc14Iert8s8Kqj0i4s2TLzm9uzFOW3CoiG5D0AskrSkkb8qujlJRlUIWMnYvXsZjDM3bqDrOyo6tXjgl0etdOxuOj23gcZ4w9Mr75IMn2TTNs4GPRS9sGewaS".split(" ").toList
     val bigData = new BigData(sqlContext, outDir, numPartitions, numRows)
    bigData.addColumn(new StringConstant("f1", "testing"))
    bigData.addColumn(new RandomLong("f2", 100000000000L))
    bigData.addColumn(new RandomLong("f3", 10000000000L))
    bigData.addColumn(new RandomLong("f4", 1000000000L))
    bigData.addColumn(new RandomDouble("f5", 1000000000.0))
    bigData.addColumn(new RandomDouble("f6", 100000000.0))
    bigData.addColumn(new RandomDouble("f7", 10000000.0))
    bigData.addColumn(new Categorical("states", states))
    bigData.addColumn(new Categorical("random_string", randomStrings))

    bigData.writeFile

  }
}
