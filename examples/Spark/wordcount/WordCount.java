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

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class WordCount
{
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

	if (args.length < 1) {
	    System.err.println("Usage: JavaWordCount <file or folder>");
	    System.exit(1);
	}

	SparkConf conf = new SparkConf().setAppName("WordCount");
	JavaSparkContext sc = new JavaSparkContext(conf);

	JavaRDD<String> lines = sc.textFile(args[0]);
	System.out.println("#partitions: " + lines.getNumPartitions());
	//lines = lines.coalesce(4);
	//System.out.println("#partitions: " + lines.getNumPartitions());

	JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
	JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
	JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
	counts.saveAsTextFile("hdfs://cscluster00.boisestate.edu:9000/user/amit/output");

	List<Tuple2<String, Integer>> output = counts.collect();
	for (Tuple2<?, ?> tuple : output) {
	    System.out.println(tuple._1() + ": " + tuple._2());
	}
	sc.stop();
	sc.close();
    }
}
