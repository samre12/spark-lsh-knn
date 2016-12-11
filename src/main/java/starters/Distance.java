package starters;

import java.util.Iterator;
import java.util.List;

public class Distance {
	static public Double ManhattanDistance(List<Double> list1, List<Double> list2) {
		Double distance = 0.0;
		Iterator<Double> iter1 = list1.iterator();
		Iterator<Double> iter2 = list2.iterator();
		while (iter1.hasNext()) {
			distance += Math.abs(iter1.next() - iter2.next());
		}
		return distance;
	}
	
	static public Double EuclideanDistance(List<Double> list1, List<Double> list2) {
		Double distance = 0.0;
		Iterator<Double> iter1 = list1.iterator();
		Iterator<Double> iter2 = list2.iterator();
		while (iter1.hasNext()) {
			distance += Math.pow((iter1.next() - iter2.next()), 2);
		}
		return Math.sqrt(distance);
	}
}
