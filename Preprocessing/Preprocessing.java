package hwk2.preprocessing;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Preprocessing {

	
	public static enum RECORDS_COUNTER {
	    NB_RECORDS,
	};

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		/*
	     * Validate that two arguments were passed from the command line.
	     */
	    if (args.length != 2) {
	      System.out.printf("Usage: StopWords <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	    /*
	     * Instantiate a Job object for your job's configuration. 
	     */
	    Configuration conf = new Configuration();
	    Job job = new Job(conf);
	    job.setJarByClass(Preprocessing.class);
	    job.setJobName("Part1_Preprocessing");
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(PreprocessingMapper.class);
	    job.setReducerClass(PreprocessingReducer.class);
	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", "; ");
	    
	    FileSystem fs = FileSystem.get(new Configuration());
	    if (fs.exists(new Path(args[1]))) {
	    	fs.delete(new Path(args[1]));
	    }
		
	    job.waitForCompletion(true);
	    
	    
	    long counter = job.getCounters().findCounter(RECORDS_COUNTER.NB_RECORDS)
				.getValue();
		Path counterFile = new Path("NB_RECORDS.txt");
		BufferedWriter bf = new BufferedWriter(new OutputStreamWriter(
				fs.create(counterFile, true)));
		bf.write(String.valueOf(counter));
		bf.close();
		
		System.exit(0);
	    
	}

}

