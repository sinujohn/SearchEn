package me.sinu.search.index;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.sinu.search.index.util.SearchConstants;
import me.sinu.search.index.util.WholeFileInputFormat;
import me.sinu.search.index.writables.IndexKey;
import me.sinu.search.index.writables.IndexTuple;
import me.sinu.search.index.writables.IndexTupleArray;
import me.sinu.search.index.writables.PositionArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Creates an Inverted Index.
 * @author Sinu John
 * www.sinujohn.wordpress.com
 *
 */
public class IndexCreator {

	/**
	 * The mapper. Input is a whole file. The mapper first inserts the filename into fileIndexTable with row key as the MD5 hash.
	 * This MD5 hash is then used instead of filename in the Inverted Index.
	 * 
	 * @author Sinu John
	 * www.sinujohn.wordpress.com
	 *
	 */
	public static class IndexMapper extends Mapper<NullWritable, BytesWritable, IndexKey, IndexTuple> {
		
		/**
		 * Reads {@code file} from Hadoop filesystem. Computes the MD5 hash of the file.
		 * Inserts the (MD5_hash, filename) into HBase table.
		 * This is used to keep track of the files.
		 * @param file
		 * @return
		 * @throws Exception
		 */
		public byte[] putFileIntoTable(Path file) throws Exception {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			MessageDigest md = MessageDigest.getInstance("MD5");
			InputStream is = fs.open(file);
			DigestInputStream dis = new DigestInputStream(is, md);
			byte[] buffer = new byte[1024];
			int numRead;
			do {
				numRead = dis.read(buffer);
			} while(numRead>0);
			dis.close();
			
			byte[] digest = md.digest();
			
			Configuration hbaseConfig = HBaseConfiguration.create();
			HTable table = new HTable(hbaseConfig, SearchConstants.FILE_INDEX_TABLE_NAME);
			Put p = new Put(digest);
			p.add(Bytes.toBytes(SearchConstants.FILE_INDEX_COL_FAMILY), Bytes.toBytes(SearchConstants.FILE_INDEX_COL_FILENAME), Bytes.toBytes(file.getName()));
			table.put(p);
			table.close();
			
			return digest;
		}
		
		private Map<String, List<LongWritable>> table = new HashMap<String, List<LongWritable>>();
		@Override
		protected void map(NullWritable key, BytesWritable value,
				Context context)
				throws IOException, InterruptedException {
			Path filename = ((FileSplit)context.getInputSplit()).getPath();
			
			BytesWritable fileIndex;
			try {
				fileIndex = new BytesWritable(putFileIntoTable(filename));
			} catch (Exception e) {
				fileIndex =new BytesWritable("ERROR".getBytes());
			}
			
			String contents = new Text(value.getBytes()).toString(); 
			String[] words = contents.split("[^a-zA-Z]");
			long count = 0;
			
			for(String word : words) {
				if(word == null || word.trim().isEmpty()) {
					continue;
				}
				word = word.trim();
				List<LongWritable> positions;
				if(table.get(word) == null) {
					positions = new ArrayList<LongWritable>();
					table.put(word, positions);
				} else {
					positions = table.get(word);
				}
				positions.add(new LongWritable(count));
				count++;
			}
			for(String keyStr : table.keySet()) {
				PositionArray arr = new PositionArray();
				arr.set(table.get(keyStr).toArray(new LongWritable[]{}));
				context.write(new IndexKey(keyStr), new IndexTuple(fileIndex, arr));
			}
		}
	}
	
	/**
	 * This Reducer which doesn't depend on HBase. This one saves the Inverted Index into a file in HDFS.
	 * @author Sinu John
	 * www.sinujohn.wordpress.com
	 *
	 */
	public static class IndexReducer extends Reducer<IndexKey, IndexTuple, Text, IndexTupleArray> {
		@Override
		protected void reduce(IndexKey key, Iterable<IndexTuple> values,
				Context context)
				throws IOException, InterruptedException {
			System.out.println("IN_REDUCER");
			IndexTupleArray arr = new IndexTupleArray();
			List<IndexTuple> list = new ArrayList<IndexTuple>();
			for(IndexTuple value : values) {
				list.add(new IndexTuple(value));
			}
			arr.set(list.toArray(new IndexTuple[list.size()]));
			
			context.write(new Text(key.toString()), arr);
		}
	}
	
	/**
	 * This reducer depends on HBase. Saves the Inverted Index into 'myInvertedIndex' table of HBase.
	 * Row key is the word and the value contains the filenames(the MD5_hash) and the positions of the words.
	 * 
	 * Known Bug: RowKey is inserted incorrectly. If keys are 'be', 'better', 'but' and 'is'; Then,
	 *  keys are inserted as 'be'(correct), 'better' (correct), 'butter' (incorrect) and 'istter'(incorrect)  
	 * @author Sinu John
	 * www.sinujohn.wordpress.com
	 *
	 */
	public static class IndexReducerHBase extends TableReducer<IndexKey, IndexTuple, Text> {
		
		@Override
		protected void reduce(IndexKey key, Iterable<IndexTuple> values,
				Context context)
				throws IOException, InterruptedException {
			
			Put put =new Put(key.getBytes());
			
			IndexTupleArray arr = new IndexTupleArray();
			List<IndexTuple> list = new ArrayList<IndexTuple>();
			for(IndexTuple value : values) {
				list.add(value);
			}
			arr.set(list.toArray(new IndexTuple[list.size()]));
			
			put.add(Bytes.toBytes(SearchConstants.COL_FAMILY), Bytes.toBytes(SearchConstants.COL_INDICES), WritableUtils.toByteArray(arr));
			
			context.write(new Text(key.toString()), put);
		}
	}
	
	/**
	 * Uses IndexReducer. Follow the instructions in the commented block inside to use the IndexReducerHBase class. 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job = new Job(conf, "invertedindex");
        
        job.setJarByClass(IndexCreator.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);
        
        //If Inverted Index is to be created in an HBase table, then
        //uncomment following lines and comment 'job.setReducerClass(IndexReducer.class);' (above line)
        //job.setReducerClass(IndexReducerHBase.class);
        //TableMapReduceUtil.initTableReducerJob(SearchConstants.INDEX_TABLE_NAME, IndexReducer.class, job);
        ////////////////////////////////////////////////////////////////////////////////////////////////////
        
        job.setMapOutputKeyClass(IndexKey.class);
        job.setMapOutputValueClass(IndexTuple.class);
        
        job.setInputFormatClass(WholeFileInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
