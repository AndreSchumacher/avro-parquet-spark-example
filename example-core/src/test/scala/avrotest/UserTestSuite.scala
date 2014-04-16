/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package avrotest

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach

import com.google.common.io.Files

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util._

import avrotest.UserOperations._

// inspired by [[org.apache.spark.LocalSparkContext]]
class UserTestSuite extends FunSuite with BeforeAndAfterEach {

  @transient var sqc: SQLContext = _

  override def beforeEach() {
    println("creating spark context")
    val conf =
      new SparkConf(false)
        .setMaster("local")
        .setAppName("test")
    sqc = new SQLContext(new SparkContext(conf))

    // Prepare some input data
    val avroFile = getTempFilePath("users", ".avro")
    val parquetFile = new Path(Files.createTempDir().toString, "users.parquet")
    // Generate some input (10 users) and write it as an Avro file to the local
    // file system
    writeAvroUsersFile(avroFile, 10)
    // Now convert the Avro file to a Parquet file (we could have generated one right away)
    convertAvroToParquetAvroUserFile(
      new Path(avroFile.toString),
      new Path(parquetFile.toString),
      sqc.sparkContext.hadoopConfiguration)

    // Import the Parquet file we just generated and register it as a table
    val schemaRdd = sqc.parquetFile(parquetFile.getParent.toString)
    schemaRdd.registerAsTable("UserTable")
  }

  override def afterEach() {
    resetSparkContext()
  }

  private def resetSparkContext() = {
    if (sqc != null) {
      println("stopping Spark Context")
      sqc.sparkContext.stop()
    }
    sqc = null
  }

  test("Favorite color") {
    assert(findFavoriteColorOfUser("User1", sqc) === "red")
  }

  test("Favorite number") {
    assert(findFavoriteNumberOfUser("User7", sqc) === 7)
  }

  test("Color distribution") {
    assert(findColorDistribution(sqc).apply("blue") === 6)
  }

  test("Best friend") {
    val friends = findInverseBestFriend(sqc).sorted
    assert(friends(0)._1 === "User10")
    assert(friends(0)._2 === "User9")
    assert(friends(1)._1 === "User2")
    assert(friends(1)._2 === "User1")
  }

  test("Buddy list") {
    val friendCount = findNumberOfFriendshipCircles(sqc)
    assert(friendCount(0)._1 === "User10")
    assert(friendCount(0)._2 === 3)
  }
}
