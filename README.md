# Big Data Maker
Spark Scala Project to assist in creating big random data datasets.

## Usage

Look in BigDataMakerApp.scala for a complete example.  Here is a snippet:

```scala
    val states = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MT NE NV NH NJ NM NY NC ND OH OK OR MD MA MI MN MS MO PA RI SC SD TN TX UT VT VA WA WV WI WY".split(" ").toList
    val bigData = new BigData(sqlContext, outDir, numPartitions, numRows)
    bigData.addColumn(new StringConstant("f1", "testing"))
    bigData.addColumn(new RandomLong("f2", 100000000000L))
    bigData.addColumn(new RandomLong("f3", 10000000000L))
    bigData.addColumn(new RandomLong("f4", 1000000000L))
    bigData.addColumn(new RandomDouble("f5", 1000000000.0))
    bigData.addColumn(new RandomDouble("f6", 100000000.0))
    bigData.addColumn(new RandomDouble("f7", 10000000.0))
    bigData.addColumn(new Categorical("states", states))

    bigData.writeFile
```
You need to specify the number of partitions (data files), and the number of rows to create per partition.  Each partition will be created in its own Spark container/thread, so the number of partitions will determine your parallelization.
