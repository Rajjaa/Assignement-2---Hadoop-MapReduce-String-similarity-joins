package hwk2.preprocessing;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PreprocessingMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
    	HashSet<String> stopwords = new HashSet<String>();
        BufferedReader Reader = new BufferedReader(new FileReader(new File("/home/cloudera/stopwords.csv")));                     
        String line;
        while ((line = Reader.readLine()) != null) {
            stopwords.add(line.split("\\s+")[0].toLowerCase());
        }
        
    	String lineF = value.toString().toLowerCase();
    	StringTokenizer tokenizer = new StringTokenizer(lineF);
    	
    	Pattern p = Pattern.compile("[^A-Za-z0-9]");
    	String token;
    	while (tokenizer.hasMoreTokens()) {
    		token = tokenizer.nextToken().toLowerCase();
    		if (!(stopwords.contains(token) || p.matcher(token).find() || token.isEmpty())){
    			word.set(token);
            	context.write(key, word);
    		}
         }
    	
       }
}


