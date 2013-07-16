package me.sinu.search.index.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Defines a single value that corresponds to a Key in Inverted Index 
 * @author Sinu John
 * www.sinujohn.wordpress.com
 *
 */
public class IndexTuple implements WritableComparable<IndexTuple> {

	private BytesWritable fileIndex;
	private PositionArray positions;

	public IndexTuple() {
		set(new BytesWritable(), new PositionArray());
	}

	/*public IndexTuple(long first, long[] second) {
		set(new BytesWritable(first), new BytesWritable(second));
	}*/

	public IndexTuple(BytesWritable first, PositionArray second) {
		set(first, second);
	}

	public IndexTuple(IndexTuple value) {
		set(new BytesWritable(value.fileIndex.getBytes()), new PositionArray(value.positions));
	}

	public void set(BytesWritable first, PositionArray second) {
		this.fileIndex = first;
		this.positions = second;
	}

	public BytesWritable getFileIndex() {
		return fileIndex;
	}

	public PositionArray getPositions() {
		return positions;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		fileIndex.write(out);
		positions.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fileIndex.readFields(in);
		positions.readFields(in);
	}

	@Override
	public int hashCode() {
		return fileIndex.hashCode() * 163 + positions.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof IndexTuple) {
			IndexTuple tp = (IndexTuple) o;
			return fileIndex.equals(tp.fileIndex) && positions.equals(tp.positions);
		}
		return false;
	}

	@Override
	public String toString() {
		return "(" + fileIndex + ":" + positions + ")";
	}

	@Override
	public int compareTo(IndexTuple tp) {
		int cmp = fileIndex.compareTo(tp.fileIndex);
		if (cmp != 0) {
			return cmp;
		}
		return positions.compareTo(tp.positions);
	}
}