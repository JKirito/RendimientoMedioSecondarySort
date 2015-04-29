package MapRed;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import entities.CeldaValueWritable;

/**
 * A partitioner ensures only that one reducer receives all the records for a
 * year; it doesn’t change the fact that the reducer groups by key within the
 * partition.
 *
 * @author pruebahadoop
 *
 */
public class CeldaPartitioner extends Partitioner<CeldaValueWritable, NullWritable> {

	@Override
	public int getPartition(CeldaValueWritable key, NullWritable value, int numPartitions) {
		// multiply by 127 to perform some mixing
		return Math.abs(key.getCeldaKey().hashCode() * 127) % numPartitions;
	}
}
