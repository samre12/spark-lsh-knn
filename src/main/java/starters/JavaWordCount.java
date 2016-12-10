package starters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class JavaWordCount {
	static Integer dim;
	static Integer k;
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Word Count"));
		dim = Integer.parseInt(args[0]);
		k = Integer.parseInt(args[1]);
		final JavaRDD<String> File = sc.textFile(args[2]);
		
		JavaPairRDD<String, List<Double>> idMap = File.mapToPair(new PairFunction<String, String, List<Double>>() {
			@Override
			public Tuple2<String, List<Double>> call(String s)
					throws Exception {
				String[] line = s.split(",");
				List<Double> val = new ArrayList<Double>(dim);
				for (int i = 1; i <= dim; i++) {
					val.add(Double.parseDouble(line[i]));
				}
				return new Tuple2<String, List<Double>>(line[0], val);
			}
		});
		idMap.cache();
		
		JavaPairRDD<String, Double> idHash = idMap.mapValues(new Function<List<Double>, Double>() {
			@Override
			public Double call(List<Double> list) throws Exception {
				return JavaWordCount.hashFunction(list);
			}
		}); 
				
		JavaPairRDD<Double, String> pairs = idHash.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
			@Override
			public Tuple2<Double, String> call(Tuple2<String, Double> t)
					throws Exception {
				return new Tuple2<Double, String>(t._2, t._1);
			}
		}); 
		pairs.cache();
		
		List<String> queries = sc.textFile(args[3]).collect();
		Iterator<String> query = queries.iterator();
		while(query.hasNext()) {
			String[] s = query.next().split(",");
			String id = s[0];
			List<Double> qval = new ArrayList<Double>(dim);
			for (int i = 1; i <= dim; i++) {
				qval.add(Double.parseDouble(s[i]));
			}
			Double qhash = hashFunction(qval);
			List<String> match = pairs.lookup(qhash);
			List<Record> dist = new ArrayList<Record>();
			for(int j = 0;j < match.size(); j++){
				List<Double> val = idMap.lookup(match.get(j)).get(0);
				dist.add(new Record(match.get(j), distance(qval, val)));
			}
			Collections.sort(dist);
			dist = dist.subList(0,k);
			System.out.println(String.format("%d Nearest Neighbors for query id %s are : \n%s\n", k, id, dist));
		}
	}
	
	private static Double distance(List<Double> list1, List<Double> list2) {
		Double d = 0.0;
		for (int i = 0; i < list1.size() ;i++) {
			d += Math.pow((list1.get(i) - list2.get(i)),2);
		}
		return Math.sqrt(d);
	}
	
	private static Double hashFunction(List<Double> list) {
		Double hash = 0.0;
		for(int i = 0;i < dim; i++)
			hash+= (list.get(i)*(Math.pow(7,i)%255))%255;
		return hash;
	}
}
