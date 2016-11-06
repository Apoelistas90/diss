package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//Any type which is to be used as a key in the Hadoop Map-Reduce framework should implement this interface.WritableComparable
public class WordPair implements Writable,WritableComparable<WordPair> {

	private Text word;
	private Text next;
	private Text s;
	
	public WordPair() {
        this.word = new Text();
        this.next = new Text();
	}
	
	
	public WordPair(String s) {
		this.word=new Text(s);
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
	    next.readFields(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
        next.write(out);		
	}
	
	@Override
    public int compareTo(WordPair other) {
        int returnVal = this.word.compareTo(other.getWord());
        if(returnVal != 0){
            return returnVal;
        }
        if(this.next.toString().equals("*")){
            return -1;
        }else if(other.getNeighbor().toString().equals("*")){
            return 1;
        }
        return this.next.compareTo(other.getNeighbor());
    }
	
	
	@Override
    public String toString() {
        return word+ " "+next;
    }

	public void setWord(String word){
        this.word.set(word);
    }
    public void setNeighbor(String neighbor){
        this.next.set(neighbor);
    }
    public Text getWord() {
        return word;
    }
    public Text getNeighbor() {
        return next;
    }
}
