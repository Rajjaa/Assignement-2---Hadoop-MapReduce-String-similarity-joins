package hwk2.setsimjoins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class DocPair implements WritableComparable<DocPair> {

	private Text key1;
    private Text key2;

    public DocPair(Text key1, Text key2) {
        set(key1, key2);
    }
    public DocPair() {
        set(new Text(), new Text());
    }

    public DocPair(String key1, String key2) {
        set(new Text(key1), new Text(key2));
    }

    public Text getFirst() {
        return key1;
    }

    public Text getSecond() {
        return key2;
    }

    public void set(Text key1, Text key2) {
        this.key1 = key1;
        this.key2 = key2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    	key1.readFields(in);
    	key2.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	key1.write(out);
    	key2.write(out);
    }

    @Override
    public String toString() {
        return key1 + " " + key2;
    }

    @Override
    public int compareTo(DocPair other) {
        int cmpFirstFirst = key1.compareTo(other.key1);
        int cmpSecondSecond = key2.compareTo(other.key2);
        int cmpFirstSecond = key2.compareTo(other.key2);
        int cmpSecondFirst = key2.compareTo(other.key1);

        if (cmpFirstFirst == 0 && cmpSecondSecond == 0 || cmpFirstSecond == 0
                && cmpSecondFirst == 0) {
            return 0;
        }

        Text thisSmaller;
        Text otherSmaller;

        Text thisBigger;
        Text otherBigger;

        if (this.key1.compareTo(this.key2) < 0) {
            thisSmaller = this.key1;
            thisBigger = this.key2;
        } else {
            thisSmaller = this.key2;
            thisBigger = this.key1;
        }

        if (other.key1.compareTo(other.key2) < 0) {
            otherSmaller = other.key1;
            otherBigger = other.key2;
        } else {
            otherSmaller = other.key2;
            otherBigger = other.key1;
        }

        int cmpThisSmallerOtherSmaller = thisSmaller.compareTo(otherSmaller);
        int cmpThisBiggerOtherBigger = thisBigger.compareTo(otherBigger);

        if (cmpThisSmallerOtherSmaller == 0) {
            return cmpThisBiggerOtherBigger;
        } else {
            return cmpThisSmallerOtherSmaller;
        }
    }

    @Override
    public int hashCode() {
        return key1.hashCode() * 163 + key2.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DocPair) {
        	DocPair tp = (DocPair) o;
            return key1.equals(tp.key1) && key2.equals(tp.key2);
        }
        return false;
    }


}

