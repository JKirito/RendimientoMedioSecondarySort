package MapRed;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import entities.CeldaValueWritable;

public class RendMedioReducer extends Reducer<CeldaValueWritable, NullWritable, CeldaValueWritable, NullWritable> {

	public void reduce(CeldaValueWritable _key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {

		context.write(_key, NullWritable.get());
	}
}
