package MapRed;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import entities.CeldaValueWritable;

public class KeyComparator extends WritableComparator {
	protected KeyComparator() {
		super(CeldaValueWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CeldaValueWritable ip1 = (CeldaValueWritable) w1;
		CeldaValueWritable ip2 = (CeldaValueWritable) w2;
		int cmp = ip1.getCeldaKey().compareTo(ip2.getCeldaKey());
		if (cmp != 0) {
			return cmp;
		}
		return -ip1.getRend().compareTo(ip2.getRend()); // reverse
	}
}
