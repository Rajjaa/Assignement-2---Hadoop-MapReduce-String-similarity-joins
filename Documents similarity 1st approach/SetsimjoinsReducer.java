package hwk2.setsimjoins;

import hwk2.setsimjoins.Setsimjoins.COUNTER;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SetsimjoinsReducer extends
		Reducer<DocPair, Text, Text, Text> {
	
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
    public void reduce(DocPair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        HashMap<String, String> allLines = new HashMap<String, String>();
        reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/SetSimJoins/preprocessing_output_sample.txt")));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] lineS = line.split(";");
            allLines.put(lineS[0], lineS[1]);
        }
        
        HashSet<String> words1 = new HashSet<String>();

        for (String word : values.iterator().next().toString().split(" ")) {
        	words1.add(word);
        }
        
        HashSet<String> words2 = new HashSet<String>();
        String doc2 = allLines.get(key.getSecond()
                .toString());
        for (String word : doc2.split(" ")) {
        	words2.add(word);
        }
        
        context.getCounter(COUNTER.NB_COMPARISIONS_I).increment(1);
        double sim = jaccardsim(words2, words2);
        
        if (sim >= 0.8) {
            context.write(new Text("(" + key.getFirst() + ", " + key.getSecond() + ")"),
                    new Text(String.valueOf(sim)));
        }
}
}

