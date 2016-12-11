package starters;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import scala.Tuple3;

public class HashFunction implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5227820063123871054L;
	private List<List<Double>> a;
	private List<Double> b;
	private Double w;
	
	HashFunction() {}
	
	HashFunction(Double w, Integer k, Integer dim, Long seed) {
		this.w = w;
		this.a = new ArrayList<List<Double>>();
		this.b = new ArrayList<Double>();
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
	
	public List<Integer> computeHash(List<Double> list) {
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
	
	private void writeObject(ObjectOutputStream oos)
			throws IOException {
		oos.defaultWriteObject();
		Tuple3<List<List<Double>>, List<Double>, Double> data = new Tuple3<List<List<Double>>, List<Double>, Double>(a, b, w);
		oos.writeObject(data);
	}
	
	private void readObject(ObjectInputStream ois)
			throws ClassNotFoundException, IOException {
		ois.defaultReadObject();
		@SuppressWarnings("unchecked")
		Tuple3<List<List<Double>>, List<Double>, Double> data = (Tuple3<List<List<Double>>, List<Double>, Double>)ois.readObject();
		a = data._1();
		b = data._2();
		w = data._3();
	}
}
