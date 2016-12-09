package starters;

import org.apache.spark.api.java.function.*;

public class GetLength implements Function<String, Integer> {
	public Integer call(String s) {
		return s.length();
	}
}
