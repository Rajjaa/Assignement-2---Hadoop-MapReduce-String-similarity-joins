package hwk2.setsimjoins2;

import hwk2.setsimjoins.DocPair;
import hwk2.setsimjoins.Setsimjoins;
import hwk2.setsimjoins.SetsimjoinsMapper;
import hwk2.setsimjoins.SetsimjoinsReducer;
import hwk2.setsimjoins.Setsimjoins.COUNTER;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Setsimjoins2 {

	public static enum COUNTER {
	    NB_COMPARISIONS_II,
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
	    job.setJarByClass(Setsimjoins.class);
	    job.setJobName("Part2_Set Similarity Joins_2nd method");
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(Setsimjoins2Mapper.class);
	    job.setReducerClass(Setsimjoins2Reducer.class);
	    
	    
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    
	    FileSystem fs = FileSystem.get(new Configuration());
	    if (fs.exists(new Path(args[1]))) {
	    	fs.delete(new Path(args[1]));
	    }
		
	    job.waitForCompletion(true);
	    
	    long counter = job.getCounters()
	            .findCounter(COUNTER.NB_COMPARISIONS_II).getValue();
	    Path outFile = new Path("NB_COMPARISIONS_II.txt");
	    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
	            fs.create(outFile, true)));
	    br.write(String.valueOf(counter));
	    br.close();
		
		System.exit(0);
	    
	}
}

