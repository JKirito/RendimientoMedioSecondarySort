package MapRed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import entities.CeldaValueWritable;

public class RendMedioDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");

//		System.setProperty("longitudMin", "-62.3161004401404");
//		System.setProperty("latitudMin", "-37.5108465428739");
		System.setProperty("anchoCelda", "0.001");

//		Context context;
//		int neighbors = context.getConfiguration().getInt("neighbors", 2);

		job.setJarByClass(RendMedioDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(RendMedioMapper.class);
		job.setPartitionerClass(CeldaPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		// TODO: specify a reducer
		job.setReducerClass(RendMedioReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(CeldaValueWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// TODO: specify input and output DIRECTORIES
		FileInputFormat.setInputPaths(job, new Path(
				"/home/pruebahadoop/Documentos/DataSets/monitores/input/monitores2.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/home/pruebahadoop/Documentos/DataSets/monitores/outputSecSort"));

		if (!job.waitForCompletion(true))
			return;
	}

}
