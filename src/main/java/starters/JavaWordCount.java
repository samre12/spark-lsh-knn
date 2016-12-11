package starters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
	static Integer distance;
	
	public static void main(String[] args) throws IllegalArgumentException{
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Word Count"));
		dim = Integer.parseInt(args[0]);
		k = Integer.parseInt(args[1]);
		l = Integer.parseInt(args[2]);
		w = Double.parseDouble(args[3]);
		threshold = Integer.parseInt(args[4]);
		distance = Integer.parseInt(args[5]);
		
		checkArguements(args);
		
		List<HashFunction> hashFunctions = new ArrayList<HashFunction>();
		for (int i = 0; i < l; i++) {
			hashFunctions.add(new HashFunction(w, k, dim, (long)i));
		}
		final Broadcast<Integer> dimBroadCast = sc.broadcast(dim);
		final Broadcast<Integer> kBroadCast = sc.broadcast(k);
		final Broadcast<Integer> lBroadCast = sc.broadcast(l);
		final Broadcast<Integer> thresholdBroadCast = sc.broadcast(threshold);
		final Broadcast<List<HashFunction>> hashFunctionsBroadcast = sc.broadcast(hashFunctions);
		
		final JavaRDD<String> File = sc.textFile(args[6]);
		
		JavaPairRDD<String, List<Double>> idMap = File.mapToPair(new PairFunction<String, String, List<Double>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 5200665490496706157L;

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
			final Broadcast<Integer> index = sc.broadcast(i);
			JavaPairRDD<List<Integer>, String> hashMap = idMap.mapToPair(new PairFunction<Tuple2<String, List<Double>>, List<Integer>, String>(){
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<List<Integer>, String> call(
						Tuple2<String, List<Double>> t) throws Exception {
					return new Tuple2<List<Integer>, String>(hashFunctionsBroadcast.value().get(index.value()).computeHash(t._2), t._1);
				}
				
			});
			hashMap.cache();
			hashMaps.add(hashMap);
		}
		
		List<String> queries = sc.textFile(args[7]).collect();
		Iterator<String> query = queries.iterator();
		List<Tuple2<String, List<Tuple2<String, Double>>>> results = new ArrayList<Tuple2<String, List<Tuple2<String, Double>>>>();
		HashSet<String> thresholdCrossId = new HashSet<String>();
		HashMap<String,Integer> idCount = new HashMap<String,Integer>();
		while(query.hasNext()) {
			String[] s = query.next().split(",");
			String id = s[0];
			List<Double> qval = new ArrayList<Double>();
			List<Integer> qhash = new ArrayList<Integer>();
			for (int i = 1; i <= dimBroadCast.value(); i++) {
				qval.add(Double.parseDouble(s[i]));
			}
			for(int n = 0; n < lBroadCast.value(); n++){
				qhash = hashFunctions.get(n).computeHash(qval);
				List<String> match = hashMaps.get(n).lookup(qhash);
				for(int i = 0;i < match.size(); i++){
					if(!idCount.containsKey(match.get(i)))
						idCount.put(match.get(i), 1);
					else{
						int cnt = idCount.get(match.get(i)) + 1;
						idCount.put(match.get(i), cnt);
						if(cnt >= thresholdBroadCast.value())
							thresholdCrossId.add(match.get(i));
					}
						
				}
			}
			
			List<Record> dist = new ArrayList<Record>();
			Iterator<String> set = thresholdCrossId.iterator();
			while(set.hasNext()){
				String recordID = set.next();
				List<Double> val = idMap.lookup(recordID).get(0);
				if (distance == 0) {
					dist.add(new Record(recordID, Distance.ManhattanDistance(qval, val)));
				} else {
					dist.add(new Record(recordID, Distance.EuclideanDistance(qval, val)));
				}
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
		resultsRDD.saveAsTextFile(args[8]);
	}
	
	private static void checkArguements(String[] args) {
		if (args.length < 9) {
			throw new IllegalArgumentException(Strings.INVALID_NUMBER);
		} else {
			if (dim <= 0) {
				throw new IllegalArgumentException(Strings.INVALID_DIM);
			} else {
				if (k < 1) {
					throw new IllegalArgumentException(Strings.INVALID_K);
				} else {
					if (l < 1) {
						throw new IllegalArgumentException(Strings.INVALID_L);
					} else {
						if (w <= 0) {
							throw new IllegalArgumentException(Strings.INVALID_W);
						} else {
							if (threshold < 1) {
								throw new IllegalArgumentException(Strings.INVALID_THRESHOLD);
							} else {
								if (distance != 0 && distance != 1) {
									throw new IllegalArgumentException(Strings.INVALID_DISTANCE);
								}
							}
						}
					}
				}
			}
		}
	}
}
