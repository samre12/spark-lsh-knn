package starters;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import java.util.*;



public class JavaWordCount {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Word Count"));
		Accumulator<Map<String, ArrayList<Integer>>> idMapAccumulator = sc.accumulator(null, "idMap", new MapAccumulator());
		
		Map<String, ArrayList<Integer>> idMap = new HashMap<String, ArrayList<Integer>>();
	}

}
