package hwk2.setsimjoins2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Setsimjoins2Mapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	    private Text word = new Text();

	    @Override
	    public void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {

	    	String doc = value.toString().split(";")[1];
	    	String docID = value.toString().split(";")[0];
	    	String[] words = doc.split(" ");
	        long keptWordsNumber = Math.round(words.length - (words.length * 0.8) + 1);
	        String[] keptWords = Arrays.copyOfRange(words, 0,(int) keptWordsNumber);
	        

	        for (String keptWord : keptWords) {
	        	
	            word.set(keptWord);
	            //System.out.println(docID);
	            context.write(word, new Text(docID));
	        }
	    }
}

