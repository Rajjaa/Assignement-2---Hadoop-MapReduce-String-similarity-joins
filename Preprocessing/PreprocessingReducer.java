package hwk2.preprocessing;
import hwk2.preprocessing.Preprocessing.RECORDS_COUNTER;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessingReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

  @Override
  public void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	// Convert words into String type and read them in a hashSet in order to remove repetition
		  ArrayList<String> tokens = new ArrayList<String>();
		  
		  for (Text word : values) {
			  tokens.add(word.toString());
			}
		  
		  HashSet<String> tokensU = new HashSet<String>(tokens);
		  
		  // Read the word count file
		  BufferedReader reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/SetSimJoins/wordcount.txt")));
		  
		  Map<String, Integer> wordcount = new HashMap<String, Integer>(); // HashMap table to store words as keys and their frequency as vales
		  String line;
			while ((line = reader.readLine()) != null) {
				String[] word = line.split("\\s+");
				if (tokensU.contains(word[0])){
					wordcount.put(word[0], Integer.parseInt(word[1]));
				}
				
			}
		  // Sort table by values
			Map<String, Integer> sortedMap = sortByValue(wordcount);
			
			// Write the ordered words in a StringBuffer
			StringBuffer bf = new StringBuffer();
			for (Entry<String, Integer> entry : sortedMap.entrySet()) {
				if(bf.length()!=0){
					bf.append(" ");
				}
				bf.append(entry.getKey());
			}
			
		    if(bf.length()!=0){
		    	context.getCounter(RECORDS_COUNTER.NB_RECORDS).increment(1);
				context.write(key, new Text(bf.toString()));
		    }
			
			
	  }
  
	  private static Map<String, Integer> sortByValue(Map<String, Integer> wordcount) {

		  List<Map.Entry<String, Integer>> list =
	              new LinkedList<Map.Entry<String, Integer>>(wordcount.entrySet());
		  
	      Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
	          public int compare(Map.Entry<String, Integer> o1,
	                             Map.Entry<String, Integer> o2) {
	              return (o1.getValue()).compareTo(o2.getValue());
	          }
	      });
	      
	      Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
	      for (Map.Entry<String, Integer> entry : list) {
	          sortedMap.put(entry.getKey(), entry.getValue());
	      }
	      return sortedMap;
	  }
	  
	  

  }

