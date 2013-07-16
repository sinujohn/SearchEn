package me.sinu.search.index.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Defines the key of an Inverted Index
 * @author Sinu John
 * www.sinujohn.wordpress.com
 *
 */
public class IndexKey implements WritableComparable<IndexKey> {

	private Text word;

	public IndexKey() {
		set(new Text());
	}

	public IndexKey(String word) {
		set(new Text(word));
	}

	public IndexKey(Text word) {
		set(word);
	}

	public IndexKey(IndexKey value) {
		set(new Text(value.word));
	}

	public void set(Text word) {
		this.word = word;
	}

	public Text getWord() {
		return word;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		word.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
	}

	@Override
	public int hashCode() {
		return word.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IndexKey) {
			IndexKey tp = (IndexKey) o;
			return word.equals(tp.word);
		}
		return false;
	}

	@Override
	public String toString() {
		return word.toString();
	}

	@Override
	public int compareTo(IndexKey tp) {
		return word.compareTo(tp.word);
	}
	
	public byte[] getBytes() {
		return word.getBytes();
	}
}