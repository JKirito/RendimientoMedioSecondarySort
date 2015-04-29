package MapRed;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import entities.CeldaValueWritable;

public class GroupComparator extends WritableComparator {
	protected GroupComparator() {
		super(CeldaValueWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CeldaValueWritable c1 = (CeldaValueWritable) w1;
		CeldaValueWritable c2 = (CeldaValueWritable) w2;
		System.out.println("c1: " + c1.toString());
		System.out.println("c2: " + c2.toString());
		System.out.println("RESULT: " + c1.getCeldaKey().compareTo(c2.getCeldaKey()));
		return c1.getCeldaKey().compareTo(c2.getCeldaKey());
	}
}