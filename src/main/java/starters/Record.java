package starters;

import java.util.Comparator;

public class Record implements Comparator<Record>,Comparable<Record>{
	public String id;
	public Double dist;
	
	Record(){}
	
	Record(String i, Double d){
		id = i;
		dist = d;
	}
	
	public int compareTo(Record d) {
		return (this.dist).compareTo(d.dist);
	}
	
	public int compare(Record d, Record d1) {
		return (int)(d.dist - d1.dist);
	}
	
	public String toString() {
		return String.format("Neighbor Id : %s", id);
	}
}