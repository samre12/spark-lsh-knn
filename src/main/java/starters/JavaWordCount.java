package starters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
		final Broadcast<Integer> dimBroadCast = sc.broadcast(dim);
		final Broadcast<Integer> kBroadCast = sc.broadcast(k);
		final Broadcast<List<HashFunction>> hashFunctionsBroadcast = sc.broadcast(hashFunctions);
		
		final JavaRDD<String> File = sc.textFile(args[2]);
		
		JavaPairRDD<String, List<Double>> idMap = File.mapToPair(new PairFunction<String, String, List<Double>>() {
			@Override
			public Tuple2<String, List<Double>> call(String s)
					throws Exception {
				String[] line = s.split(",");
				ArrayList<Double> val = new ArrayList<Double>();
				for (int i = 1; i <= dimBroadCast.value(); i++) {
					val.add(Double.parseDouble(line[i]));
				}
				return new Tuple2<String, List<Double>>(line[0], val);
			}
		});
		idMap.cache();
		
		List<JavaPairRDD<List<Integer>, String>> hashMaps = new ArrayList<JavaPairRDD<List<Integer>, String>>();
		for (int i = 0; i < l; i++) {
			Broadcast<Integer> index = sc.broadcast(i);
			hashMaps.add(idMap.mapToPair(new PairFunction<Tuple2<String, List<Double>>, List<Integer>, String>(){
				@Override
				public Tuple2<List<Integer>, String> call(
						Tuple2<String, List<Double>> t) throws Exception {
					
					return null;
				}
				
			}));
		}
		
		List<String> queries = sc.textFile(args[3]).collect();
		Iterator<String> query = queries.iterator();
		List<Tuple2<String, List<Tuple2<String, Double>>>> results = new ArrayList<Tuple2<String, List<Tuple2<String, Double>>>>();
		while(query.hasNext()) {
			String[] s = query.next().split(",");
			String id = s[0];
			List<Double> qval = new ArrayList<Double>();
			for (int i = 1; i <= dimBroadCast.value(); i++) {
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
			int neighbors = Math.min((int)kBroadCast.value(), dist.size());
			dist = dist.subList(0, neighbors);
			List<Tuple2<String, Double>> neighborsPairs = new ArrayList<Tuple2<String, Double>>();
			Iterator<Record> neighbor = dist.iterator();
			while (neighbor.hasNext()) {
				Record record = neighbor.next();
				neighborsPairs.add(new Tuple2<String, Double>(record.id, record.dist));
			}
			results.add(new Tuple2<String, List<Tuple2<String, Double>>>(id, neighborsPairs));
		}
		JavaPairRDD<String, List<Tuple2<String, Double>>> resultsRDD = sc.parallelizePairs(results);
		resultsRDD.saveAsTextFile(args[4]);
	}
}
