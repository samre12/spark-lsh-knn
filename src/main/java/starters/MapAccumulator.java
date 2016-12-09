package starters;

import org.apache.spark.AccumulatorParam;
import java.io.Serializable;
import java.util.*;

public class MapAccumulator implements AccumulatorParam<Map<String, ArrayList<Integer>>>, Serializable{

	@Override
	public Map<String, ArrayList<Integer>> addInPlace(
			Map<String, ArrayList<Integer>> m1,
			Map<String, ArrayList<Integer>> m2) {
		
		return mergeMap(m1, m2);
	}

	@Override
	public Map<String, ArrayList<Integer>> zero(
			Map<String, ArrayList<Integer>> m) {
		
		return new HashMap<String, ArrayList<Integer>>();
	}

	@Override
	public Map<String, ArrayList<Integer>> addAccumulator(
			Map<String, ArrayList<Integer>> m1,
			Map<String, ArrayList<Integer>> m2) {

		return mergeMap(m1, m2);
	}
	
	private Map<String, ArrayList<Integer>> mergeMap (Map<String, ArrayList<Integer>> m1, Map<String, ArrayList<Integer>> m2) {
		Map<String, ArrayList<Integer>> output = new HashMap<String, ArrayList<Integer>>(m1);
		output.putAll(m2);
		return output;
	}
}
