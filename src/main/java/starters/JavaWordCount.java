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
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class JavaWordCount {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Word Count"));
		final Integer dim = Integer.parseInt(args[0]); 
		final JavaRDD<String> File = sc.textFile(args[1]);
//		final JavaRDD<String> words = File.flatMap(new FlatMapFunction<String, String>() {
//			  public Iterable<String> call(String s) { return Arrays.asList(s.split(",")); }
//			});
		//JavaSQLContext sqlContext = new JavaSQLContext(sc);
//		JavaRDD<String[]> rdd_records = File.map(
//				  new Function<String, String[]>() {
//				      public String[] call(String line) throws Exception {
//				    	  String[] fields = line.split(",");
//				         return fields;
//				      }
//				});
		
		//HashMap<String,String> hmap = new HashMap<String,String>();
				
		
		JavaPairRDD<Double, String> pairs = File.mapToPair(new PairFunction<String, Double,String>() {
			  public Tuple2<Double,String> call(String s) { 
				  String[] line= s.split(",");
				  //Integer[] val = new Integer[dim];
				  List<Integer> val = new ArrayList<Integer>(dim);
				  for (int i=1;i<=dim;i++)
				   val.add(Integer.parseInt(line[i]));
 				  Double hash=0.0;
 				  for(int i=0;i<dim;i++)
 					  hash+= (val.get(i)*(Math.pow(7,i)%255))%255;
				  return new Tuple2<Double,String>(hash,line[0]); }
			});
		
		JavaPairRDD<Double,Iterable<String> > htable = pairs.groupByKey().cache();
		
		final JavaRDD<String> Query = sc.textFile(args[2]);
		JavaPairRDD <Double, String> queryHash = Query.mapToPair(new PairFunction<String, Double,String>(){
			public Tuple2<Double,String> call(String s) { 
				  String[] line= s.split(",");
				  //Integer[] val = new Integer[dim];
				  List<Integer> val = new ArrayList<Integer>(dim);
				  for (int i=1;i<=dim;i++)
				   val.add(Integer.parseInt(line[i]));
				  Double hash=0.0;
				  for(int i=0;i<dim;i++)
					  hash+= (val.get(i)*(Math.pow(7,i)%255))%255;
				  return new Tuple2<Double,String>(hash,line[0]); }
		}); 
		
		
		JavaPairRDD<Double,Iterable<String> > bucket = htable.filter(new Function<Tuple2<Double,Iterable<String> >,Boolean >(){
			public Boolean call(Tuple2<Double,Iterable<String> > tup){
				if(tup._1()==qhash)
					return true;
				else
					return false;
			}
		});
		
		JavaPairRDD<String,String> fin = bucket.flatMapToPair(new PairFlatMapFunction<Tuple2<Double,Iterable<String> >,String, String >(){
			public
		});
				
}
}
