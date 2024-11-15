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

import java.io.Serializable;
// $example off:create_ds$

// $example on:create_df$
// $example on:run_sql$
// $example on:programmatic_schema$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example on:init_session$
import org.apache.spark.sql.SparkSession;
// $example off:programmatic_schema$
import org.apache.spark.sql.AnalysisException;

// $example on:untyped_ops$
// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;

public class JavaSparkSQLExample1 {
  public static class Person implements Serializable {
	private static final long serialVersionUID = -8952063174688156233L;
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

  public static void main(String[] args) throws AnalysisException {
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL example 1")
      .config("spark.some.config.option", "some-value")
      .getOrCreate();

    runBasicDataFrameExample(spark);

    spark.stop();
  }

  private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
    Dataset<Row> df = spark.read().json("people.json");

    df.show();
    
    // Print the schema in a tree format
    df.printSchema();

    // Select only the "name" column
    df.select("name").show();

    // Select everybody, but increment the age by 1
    df.select(col("name"), col("age").plus(1)).show();
   
    // Select people older than 21
    df.filter(col("age").gt(21)).show();
    

    // Count people by age
    df.groupBy("age").count().show();
   
    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people");

    Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
    sqlDF.show();
 
    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people");

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show();

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show();
  }
}
