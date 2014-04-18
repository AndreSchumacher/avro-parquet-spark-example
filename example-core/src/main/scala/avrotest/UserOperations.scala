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

import java.io.File

import scala.util.Random

import org.apache.hadoop.fs.Path

import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.mapred.FsInput
import org.apache.avro.specific.{SpecificRecord, SpecificDatumReader, SpecificDatumWriter}

import parquet.avro.AvroParquetWriter

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.avro.generic.{GenericDatumReader, IndexedRecord}
import org.apache.avro.Schema

// our own class generated from user.avdl by Avro tools
import avrotest.avro.{Message, User}

// Implicits
import collection.JavaConversions._

object UserOperations {

  var random: Random = new Random(15485863l)

  /**
   * For the given username, find the favorite number of the user.
   *
   * @param name User name
   * @param sqc The SQLContext to use
   * @return The user's age
   */
  def findAgeOfUser(name: String, sqc: SQLContext): Int = {
    sqc.sql("SELECT age FROM UserTable WHERE name = \"" + name + "\"")
      .collect()
      .apply(0)
      .apply(0)
      .asInstanceOf[Int]
  }

  /**
   * For the given username, find the favorite color of the user.
   *
   * @param name User name
   * @param sqc The SQLContext to use
   * @return The favorite color
   */
  def findFavoriteColorOfUser(name: String, sqc: SQLContext): String = {
    sqc.sql("SELECT favorite_color FROM UserTable WHERE name = \"" + name + "\"")
      .collect()
      .apply(0)
      .apply(0)
      .asInstanceOf[String]
  }

  /**
   * For every color that is contained in the table, find the number of
   * users whose favorite color matches that color.
   *
   * @param sqc The SQLContext to use
   * @return A map of colors to the number of its occurrences
   */
  def findColorDistribution(sqc: SQLContext): Map[String, Int] = {
    val result = new collection.mutable.HashMap[String, Int]()
    val colorCounts = sqc.sql("SELECT favorite_color, COUNT(name) FROM UserTable GROUP BY favorite_color")
      .collect()
    for(row <- colorCounts) {
      result += row.getString(0) -> row.getInt(1)
    }
    result.toMap
  }

  /**
   * For each user in the UserTable find the number of messages sent in the
   * MessageTable.
   *
   * @param sqc The SQLContext to use
   * @return A list of pairs (user name, number of messages sent by that user)
   */
  def findNumberOfMessagesSent(sqc: SQLContext): Seq[(String, Int)] = {
   sqc.sql("""
        SELECT name, COUNT(recipient) FROM
          UserTable JOIN MessageTable ON UserTable.name = MessageTable.sender
            GROUP BY name ORDER BY name""")
     .collect()
     .map(row => (row.getString(0), row.getInt(1)))
  }

  /**
   * Find all pairs of users from the MessageTable that have mutually exchanged
   * messages. Note: this may report duplicate pairs (User1, User2) and (User2, User1).
   *
   * @param sqc The SQLContext to use
   * @return A list of pairs (user name, user name)
   */
  def findMutualMessageExchanges(sqc: SQLContext): Seq[(String, String)] = {
    sqc.sql("""
        SELECT DISTINCT A.sender, B.sender FROM
          (SELECT sender, recipient FROM MessageTable) A
        JOIN
          (SELECT sender, recipient FROM MessageTable) B
        ON A.recipient = B.sender AND A.sender = B.recipient""")
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
  }

  /**
   * Counter the number of occurences of each word contained in a message in
   * the MessageTable and returns the result as a word->count Map.
   * 
   * @param sqc he SQLContext to use
   * @return A Map that has the words as key and the count as value
   */
  def countWordsInMessages(sqc: SQLContext): Map[String, Int] = {
    sqc.sql("SELECT content from MessageTable")
      .flatMap(row =>
        row.getString(0).replace(",", "").split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()
      .toMap
  }

  /**
   * Generates a of Avro objects and stores them inside an Avro file.
   *
   * @param file The output file
   * @param factoryMethod The function to call to actually create the objects
   * @param count The number of objects to generate
   */
  def writeAvroFile[T <: SpecificRecord](
      file: File,
      factoryMethod: Int => T,
      count: Int): Unit = {
    val prototype = factoryMethod(0)
    val datumWriter = new SpecificDatumWriter[T](
      prototype.getClass.asInstanceOf[java.lang.Class[T]])
    val dataFileWriter = new DataFileWriter[T](datumWriter)

    dataFileWriter.create(prototype.getSchema, file)
    for(i <- 1 to count) {
      dataFileWriter.append(factoryMethod(i))
    }
    dataFileWriter.close()
  }

  /**
   * Converts an Avro file that contains a set of Avro objects to a Parquet file.
   *
   * @param input The Avro file to convert
   * @param output The output file (possibly on HDFS)
   * @param schema The Avro schema of the input file
   * @param conf A Hadoop `Configuration` to use
   */
  def convertAvroToParquetAvroFile(
      input: Path,
      output: Path,
      schema: Schema,
      conf: Configuration): Unit = {
    val fsInput = new FsInput(input, conf)
    val reader =  new GenericDatumReader[IndexedRecord](schema)
    val dataFileReader = DataFileReader.openReader(fsInput, reader)
    val parquetWriter = new AvroParquetWriter[IndexedRecord](output, schema)

    while(dataFileReader.hasNext)  {
      // Mote: the writer does not copy the passed object and buffers
      // writers. Therefore we need to pass a new User object every time,
      // although the reader allows us to re-use the same object.
      parquetWriter.write(dataFileReader.next())
    }

    dataFileReader.close()
    parquetWriter.close()
  }

  /**
   * Creates a User Avro object.
   *
   * @param id The ID of the user to generate
   * @return An Avro object that represents the user
   */
  def createUser(id: Int): User = {
    val builder = User.newBuilder()
      .setName(s"User$id")
      .setAge(id / 10)
    if (id >= 5) {
      builder
        .setFavoriteColor("blue")
        .build()
    } else {
      builder
        .setFavoriteColor("red")
        .build()
    }
  }

  /**
   * Creates a Message Avro object.
   *
   * @param id The ID of the message to generate
   * @return an Avro object that represents the mssage
   */
  def createMessage(maxUserId: Int)(id: Int): Message = {
    val sender = random.nextInt(maxUserId)
    var recipient = random.nextInt(maxUserId)
    while (recipient == sender) recipient = random.nextInt(maxUserId)

    Message.newBuilder()
      .setID(id)
      .setSender(s"User$sender")
      .setRecipient(s"User$recipient")
      .setContent(s"Hey there, User$recipient, this is me, User$sender")
      .build()
  }
}
