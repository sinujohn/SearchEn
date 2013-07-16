package me.sinu.search.index.writables;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Stores the positions of a word in a file as an array
 * @author Sinu John
 * www.sinujohn.wordpress.com
 *
 */
public class PositionArray extends ArrayWritable implements Comparable<PositionArray> {

	public PositionArray() {
		super(LongWritable.class);
	}
	
	public PositionArray(PositionArray arr) {
		super(LongWritable.class);
		Writable[] writables = arr.get();
		set(Arrays.copyOf(writables, writables.length));
	}
	
	@Override
	public String toString() {
		return Arrays.toString(get());
	}

	@Override
	public int compareTo(PositionArray o) {
		LongWritable[] otherPos = (LongWritable[]) o.get();
		LongWritable[] pos = (LongWritable[]) this.get();
		int cmp;
		int i;
		for(i=0; i<pos.length && i<otherPos.length; i++) {
			cmp = pos[i].compareTo(otherPos[i]);
			if(cmp != 0) {
				return cmp;
			}
		}
		if(i == pos.length && i==otherPos.length) {
			cmp = 0;
		} else if(i == pos.length) {
			cmp = -1;
		} else {
			cmp = 1;
		}
		return cmp;
	}

}
