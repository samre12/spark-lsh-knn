package starters;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import java.util.*;
import org.apache.spark.api.java.*;
import scala.Tuple2;

public class JavaWordCount {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Word Count"));
		
		JavaRDD<String> lines = sc.textFile(args[0]);
		JavaRDD<Integer> lineLengths = lines.map(new GetLength());
		int totalLength = lineLengths.reduce(new Sum());
		
	}
}
