package info.hb.hbase.client.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 可配值Mapper数量的输入格式，当写入Mapper没有任何真实的输入（例如，Mapper是输出的是随机生成的数据）时该格式比较有用。
 *
 * @author wanggang
 *
 */
public class NumMapperInputFormat extends InputFormat<NullWritable, NullWritable> {

	private static final String NMAPS_KEY = "inputformat.num.maps";

	@Override
	public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split,
			TaskAttemptContext tac) throws IOException, InterruptedException {
		return new SingleRecordReader<NullWritable, NullWritable>(NullWritable.get(),
				NullWritable.get());
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		int count = getNumMapTasks(context.getConfiguration());
		List<InputSplit> splits = new ArrayList<InputSplit>(count);
		for (int i = 0; i < count; i++) {
			splits.add(new NullInputSplit());
		}
		return splits;
	}

	public static void setNumMapTasks(Configuration conf, int numTasks) {
		conf.setInt(NMAPS_KEY, numTasks);
	}

	public static int getNumMapTasks(Configuration conf) {
		return conf.getInt(NMAPS_KEY, 1);
	}

	private static class NullInputSplit extends InputSplit implements Writable {

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] {};
		}

		@Override
		public void readFields(DataInput in) throws IOException {
		}

		@Override
		public void write(DataOutput out) throws IOException {
		}

	}

	private static class SingleRecordReader<K, V> extends RecordReader<K, V> {

		private final K key;
		private final V value;
		boolean providedKey = false;

		SingleRecordReader(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public void close() {
		}

		@Override
		public K getCurrentKey() {
			return key;
		}

		@Override
		public V getCurrentValue() {
			return value;
		}

		@Override
		public float getProgress() {
			return 0;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext tac) {
		}

		@Override
		public boolean nextKeyValue() {
			if (providedKey)
				return false;
			providedKey = true;
			return true;
		}

	}

}
