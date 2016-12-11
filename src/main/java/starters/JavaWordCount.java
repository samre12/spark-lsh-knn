package starters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class JavaWordCount {
	static Integer dim;
	static Integer k;
	static Integer l;
	static Double w;
	static Integer threshold;
	
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Word Count"));
		dim = Integer.parseInt(args[0]);
		k = Integer.parseInt(args[1]);
		l = Integer.parseInt(args[2]);
		w = Double.parseDouble(args[3]);
		threshold = Integer.parseInt(args[4]);
		
		List<HashFunction> hashFunctions = new ArrayList<HashFunction>();
		for (int i = 0; i < l; i++) {
			hashFunctions.add(new HashFunction(w, k, dim, (long)i));
		}
		Broadcast<List<HashFunction>> hashFunctionsBroadcast = sc.broadcast(hashFunctions);
	}
}
