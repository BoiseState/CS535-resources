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


/* Modified by Amit */

// $example off:programmatic_schema$
// $example on:create_ds$
import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;
// $example off:create_ds$

// $example off:programmatic_schema$
// $example on:create_ds$
import org.apache.spark.api.java.function.MapFunction;
// $example on:create_df$
// $example on:run_sql$
// $example on:programmatic_schema$
import org.apache.spark.sql.Dataset;
// $example off:programmatic_schema$
// $example off:create_df$
// $example off:run_sql$
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
// $example on:init_session$
import org.apache.spark.sql.SparkSession;
// $example off:programmatic_schema$
import org.apache.spark.sql.AnalysisException;

public class JavaSparkSQLExample2 {
  // $example on:create_ds$
  public static class Person implements Serializable {
    private String name;
    private int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }
  // $example off:create_ds$

  public static void main(String[] args) throws AnalysisException {
    // $example on:init_session$
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate();
    // $example off:init_session$

    runDatasetCreationExample(spark);

    spark.stop();
  }

  private static void runDatasetCreationExample(SparkSession spark) {
    // $example on:create_ds$
    // Create an instance of a Bean class
    Person person = new Person();
    person.setName("Andy");
    person.setAge(32);

    // Encoders are created for Java beans
    Encoder<Person> personEncoder = Encoders.bean(Person.class);
    Dataset<Person> javaBeanDS = spark.createDataset(
      Collections.singletonList(person),
      personEncoder
    );
    javaBeanDS.show();
    // +---+----+
    // |age|name|
    // +---+----+
    // | 32|Andy|
    // +---+----+

    // Encoders for most common types are provided in class Encoders
    Encoder<Integer> integerEncoder = Encoders.INT();
    Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
    Dataset<Integer> transformedDS = primitiveDS.map(
        (MapFunction<Integer, Integer>) value -> value + 1,
        integerEncoder);
    transformedDS.collect(); // Returns [2, 3, 4]

    // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
    String path = "people.json";
    Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
    peopleDS.show();
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_ds$
  }
}
