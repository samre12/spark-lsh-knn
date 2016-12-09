package starters;

import org.apache.spark.api.java.function.*;

public class Sum implements Function2<Integer, Integer, Integer>{
	public Integer call(Integer a, Integer b) {
		return a + b;
	}
}
