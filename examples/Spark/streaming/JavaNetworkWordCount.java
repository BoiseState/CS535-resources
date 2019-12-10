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

/* modified by amit */

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network
 * every second.
 *
 * Usage: JavaNetworkWordCount <hostname> <port> <hostname> and <port> describe
 * the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server `$
 * nc -lk 9999` and then run the example `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
 */
public final class JavaNetworkWordCount
{
    private static final Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args) throws Exception {
	if (args.length < 2) {
	    System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
	    System.exit(1);
	}

	SparkSession spark = SparkSession.builder().appName("JavaStructuredNetworkWordCount").getOrCreate();

	// Create DataFrame representing the stream of input lines from connection to
	// localhost:9999
	Dataset<Row> lines = spark.readStream().format("socket").option("host", "localhost").option("port", 9999)
	        .load();

	// Split the lines into words
	Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
	        (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

	// Generate running word count
	Dataset<Row> wordCounts = words.groupBy("value").count();

	// Start running the query that prints the running counts to the console
	StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

	query.awaitTermination();

    }
}
