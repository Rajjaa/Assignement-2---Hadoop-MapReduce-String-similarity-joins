package hwk2.setsimjoins2;

import hwk2.setsimjoins2.Setsimjoins2.COUNTER;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Setsimjoins2Reducer extends Reducer<Text, Text, Text, Text> {
	
    private BufferedReader reader;
	
    public double jaccardsim(HashSet<String> v1, HashSet<String> v2) {
    	
    	HashSet<String> intersect1 = v1;
    	intersect1.retainAll(v2);
    	int intertsect = intersect1.size();
    	
        if (v1.size() < v2.size()) {
        	HashSet<String> unionSet = v1;
        	unionSet.addAll(v2);
        	int union = unionSet.size();
            return (double) intertsect / union;
        } else {
        	HashSet<String> unionSet = v2;
            unionSet.addAll(v1);
            int union = unionSet.size();
            return (double) intertsect / union;
        }
    }
    
    @Override
	public void reduce (Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

	    HashMap<String, String> allLines = new HashMap<String, String>();
        reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/SetSimJoins/preprocessing_output_sample.txt")));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] lineS = line.split(";");
            allLines.put(lineS[0], lineS[1]);
        }
        
        List<String> docSet = new ArrayList<String>();
        for (Text id : values){
        	docSet.add(id.toString());
        }
        
	    //System.out.println(docSet[0]);
	    
	    if (docSet.size() > 1) {
	        ArrayList<String> pairs = new ArrayList<String>();
	        for (int i = 0; i < docSet.size(); ++i) {
	            for (int j = i + 1; j < docSet.size(); ++j) {
	                String pair = new String(docSet.get(i) + " "
	                        + docSet.get(j));
	                pairs.add(pair);
	            }
	        }
	        //System.out.println("******************");
	        //System.out.println(pairs.size());
	        for (String pair : pairs) {
	        	HashSet<String> words11 = new HashSet<String>();
	            String words12 = allLines.get(pair.split(" ")[0].toString());
	            for (String word : words12.split(" ")) {
	                words11.add(word);
	            }

	            HashSet<String> words21 = new HashSet<String>();
	            String words22 = allLines.get(pair.split(" ")[1].toString());
	            for (String word : words22.split(" ")) {
	            	words21.add(word);
	            }

	            context.getCounter(COUNTER.NB_COMPARISIONS_II).increment(1);
	            double sim = jaccardsim(words11,
	            		words21);	
	            System.out.println("*************");
	            if (sim >= 0.1) {
	            	System.out.println(pair.split(" ")[0]);
	                context.write(new Text("(" + pair.split(" ")[0] + ", "
	                        + pair.split(" ")[1] + ")"),
	                        new Text(String.valueOf(sim)));
	            }
	        }
	        
	        
	    }
	    
	    
	}
}

	               
	     
	 


