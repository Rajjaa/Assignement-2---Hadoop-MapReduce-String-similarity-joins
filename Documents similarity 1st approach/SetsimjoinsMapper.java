package hwk2.setsimjoins;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SetsimjoinsMapper extends Mapper<LongWritable, Text, DocPair, Text> {

	private BufferedReader reader;
    private static DocPair pairKeys = new DocPair();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        reader = new BufferedReader(new FileReader(new File("/home/cloudera/workspace/SetSimJoins/preprocessing_output_sample.txt")));
        String line;
        System.out.println(value.toString());
        String[] valueS = value.toString().split(";");
        while ((line = reader.readLine()) != null) {
            String key2 = line.split(";")[0];
            if (!key.toString().equals(key2)){
            	pairKeys.set(new Text(valueS[0]), new Text(key2));
            	context.write(pairKeys, new Text(valueS[1]));
            }
        }
        }

}
    


