Spark SQL, Avro and Parquet
===========================

This tutorial shows how to query data stored as Avro objects stored
inside a columnar format (Parquet) via the Spark SQL query
interface. The main intention of the tutorial is to show the seamless
integration of the functional RDD operators that come with Spark and
its SQL interface. For users who are unfamiliar with Avro we show how
to make use of Avro interface description language (IDL) inside a
Spark Maven project.

_Acknowledgments: Thanks to Matt Massie (@massie) and the ADAM
project for showing how to integrate Avro and Parquet with Spark._

Building the example
--------------------

```
$ git clone https://github.com/apache/spark.git
$ cd spark
$ git pull https://github.com/AndreSchumacher/spark.git nested_parquet
$ sbt/sbt clean publish-local
```

Then in a different directory

```
$ git clone https://github.com/AndreSchumacher/avro-parquet-spark-example.git
$ cd avro-parquet-spark-example
$ mvn package
```

Other dependencies
------------------

Currently the example code of this tutorial requires a patched version
of the master branch of Spark (pre version 1.1) and is therefore not
intended to be used by the general public.

Note: requires merge of https://github.com/apache/spark/pull/360

Project setup
-------------

Here we are using Maven to build the project due to the available Avro
IDL compiler plugin. Obviously one could have achieved the same goal
using sbt.

There are two subprojects:

* `example-format`, which contains the Avro description of the primary
  data record we are using (`User`)
* `example-code`, which contains the actual code that executes the
  queries

There are two ways to specify a schema for Avro records: via a
description in JSON format or via the IDL.  We chose the latter since
it is easier to comprehend.

Our example models the user database of a social network, where users
are asked to complete a personal profile which contains among other
things their name and favorite color. Users are also asked to add
other users to their buddy list. The schema of the resulting `User`
records then looks as follows.

```xml
@namespace("avrotest.avro")
protocol AvroSparkSQL {
    record User {
        // The name of the user
        string name;
        // The favorite number of the user
        int favorite_number = 0;
        // The favorite color of the user
        string favorite_color = null;
        // The list of friends of the user
        array<string> friends;
    }
}
```

This file is stored as part of the `example-format` project and is
eventually compiled into a Java implementation of the class that
represents this record. Note that part of the description is also the
_namespace_ of the protocol, which will result in the package name of
the classes that will be generated from the description. We use the
Avro maven plugin to do this transformation. It is added to
`example-format/pom.xml` as follows:

```xml
<plugin>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-maven-plugin</artifactId>
</plugin>
```

Data generation
---------------

Once the code generation has completed, objects of type `User` can be
created via the `Builder` that was generated. For example:

```Scala
User.newBuilder()
    .setName("User1")
    .setFavoriteNumber(42)
    .setFriends(List("User4", "User7"))
    .setFavoriteColor("blue")
    .build()
```

We can create a set of users and store these inside an Avro file as
follows.

```Scala
import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter

val userDatumWriter = new SpecificDatumWriter[User](classOf[User])
val dataFileWriter = new DataFileWriter[User](userDatumWriter)
dataFileWriter.create(User.getClassSchema, file)

for(i <- 1 to numberOfUsers) {
    dataFileWriter.append(createUser(i))
}

dataFileWriter.close()
```

It is generally also possible to skip the step of code generation (for
example, when the schema is generated dynamically). In this case there
is similar but different approach using generic writers to write data
to Avro files. Note that `createUser` in the above example is a
factory method that uses the `Builder` to create `User` objects as
described above.

Data stored in Avro format has the advantage of being accessible from
a large number of programming languages and frameworks for which there
exist Avro code generators. In order to process it via columnar
frameworks such as Parquet we need to convert the data first (or store
it in Parquet format right away). This tutorial assumes that you want
to convert Avro files to Parquet files stored inside, say, HDFS. The
conversion can be achieved as follows.

```Scala
import org.apache.avro.file.DataFileReader
import org.apache.avro.mapred.FsInput
import org.apache.avro.specific.SpecificDatumReader
import parquet.avro.AvroParquetWriter

val fsInput = new FsInput(input, conf)
val reader =  new SpecificDatumReader[User](classOf[User])
val dataFileReader = DataFileReader.openReader(fsInput, reader)
val parquetWriter = new AvroParquetWriter[User](output, User.getClassSchema)

while(dataFileReader.hasNext)  {
    parquetWriter.write(dataFileReader.next())
}

dataFileReader.close()
parquetWriter.close()
```

Import into Spark SQL
---------------------

The data written in the last step can be directly imported as a table
inside Spark SQL and then queried. This can be done as follows.

```Scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

val conf = new SparkConf(true)
    .setMaster("local")
    .setAppName("ParquetAvroExample")
val sqc = new SQLContext(new SparkContext(conf))

val schemaRdd = sqc.parquetFile(parquetFile.getParent.toString)
    schemaRdd.registerAsTable("UserTable")
```

Here `parquetFile` is the path under which the previously generated
Parquet file was stored. Note that we do not need to specify a schema,
since the schema is preserved all the way from the initial
specification in Avro IDL to the registration as a table inside Spark
SQL.

Querying the user database
--------------------------

After the table has been registered, it can queried via SQL syntax, for
example:

```Scala
sqc.sql("SELECT favorite_color FROM UserTable WHERE name = \"User5\"").collect()
```

Also more complicated operations can be performed, for example:

```Scala
sqc.sql("SELECT name, friends FROM UserTable")
    .registerAsTable("UserFriends")
sqc.sql("SELECT UserTable.name, UserFriends.name FROM UserTable JOIN UserFriends ON UserTable.name = UserFriends.friends[0]")
```

The last example will generate a list of pairs of usernames, such that
the first username will appear in the first position of the second
user name's friend list. Thus, it creates a list of (not necessarily
mutual) best-friend relations.

Mixing SQL and other Spark operations
-------------------------------------

Since SQL data is stored as RDDs that have a schema attached to them
(hence, `SchemaRDD`), SQL and other operations on RDDs can be mixed freely,
for example:

```Scala
sqc.sql("SELECT friends FROM UserTable")
    .flatMap(row => row(0).asInstanceOf[Row].seq)
    .groupBy(_.asInstanceOf[String])
    .map(pair => (pair._1, pair._2.size))
    .sortByKey()
    .collect()
```

The previous example counts the number of times a user appears on
another user's friend list.
