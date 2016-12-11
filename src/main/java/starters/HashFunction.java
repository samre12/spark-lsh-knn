package starters;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HashFunction {
	public List<List<Double>> a;
	public List<Double> b;
	public Double w;
	
	HashFunction() {}
	
	HashFunction(Double w, Integer k, Integer dim, Long seed) {
		this.w = w;
		Random rand = new Random(seed);
		for (int i = 0; i < k; i++) {
			b.add(rand.nextDouble() * w);
			List<Double> aTemp = new ArrayList<Double>();
			//check Gaussian
			for (int j = 0; j < dim; j++) {
				aTemp.add(rand.nextGaussian());
			}
			a.add(aTemp);
		}
	}
	
	HashFunction(List<List<Double>> a, List<Double> b, Double w) {
		this.a = a;
		this.b = b;
		this.w = w;
	}
	
	private List<Integer> computeHash(List<Double> list) {
		List<Integer> hash = new ArrayList<Integer>();
		for (int i = 0; i < a.size(); i++) {
			Double hashTemp = 0.0;
			for (int j = 0; j < list.size(); j++) {
				hashTemp += list.get(j) * a.get(i).get(j);
			}
			hashTemp += b.get(i);
			hashTemp = hashTemp / w;
			hash.add((int)Math.floor((hashTemp)));
		}
		return hash;
	}
}
