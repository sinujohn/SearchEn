package me.sinu.search.index.writables;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;

/**
 * Holds an array of IndexTuple.
 * Inverted Index table contains: [IndexKey, IndexTupleArray]
 * @author Sinu John
 * www.sinujohn.wordpress.com
 *
 */
public class IndexTupleArray extends ArrayWritable {

	public IndexTupleArray() {
		super(IndexTuple.class);
	}
	
	@Override
	public String toString() {
		return Arrays.toString(get());
	}

}
