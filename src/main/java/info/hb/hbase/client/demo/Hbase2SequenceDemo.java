package info.hb.hbase.client.demo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * 将HBase中的数据表导出逗号分割的序列化文件
 *
 * 在CDH5上运行命令：
 * 1、在root用户下操作
 * $ mv apt-hdfs-1.0.0-jar-with-dependencies.jar /var/lib/hadoop-hdfs/
 * 2、在hdfs用户下操作
 * $ su - hdfs
 * $ hadoop jar apt-hdfs-1.0.0-jar-with-dependencies.jar zx.soft.apt.hdfs.hbase.demo.Hbase2SequenceDemo
 * $ hadoop fs -text /user/wanggang/hbase2sequence/part-m-00000 | less
 *
 * 性能测试结果：1亿条数据运行10分钟左右
 *
 * @author wanggang
 *
 */
public class Hbase2SequenceDemo {

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		String outputPath = "/user/wanggang/hbase2sequence";

		// 配置HDFS
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "kafka04,kafka05,kafka06,kafka07,kafka08");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		// 创建作业
		Job job = Job.getInstance(conf);
		job.setJarByClass(Hbase2SequenceDemo.class);
		job.setJobName("Hbase2SequenceDemo");

		// 创建并设置Scan
		Scan scan = new Scan();
		// 默认是1，但是MapReduce作业不适合这个默认值
		scan.setCaching(500);
		// MapReducer作业不能设置True
		scan.setCacheBlocks(Boolean.FALSE);

		// 初始化作业使用的数据表和Mapper
		TableMapReduceUtil.initTableMapperJob("user_info_test", scan, Hbase2SequenceMapper.class,
				null, null, job);
		job.setNumReduceTasks(0);

		// 作业输出格式
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // GzipCodec/SnappyCodec
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new IOException("error with job");
		}
	}

	public static class Hbase2SequenceMapper extends TableMapper<Text, NullWritable> {

		Text text = new Text();
		static final String COLUMN_FAMILYS = "id_col,base_col,education_col";
		static final String QUALIFIER_NAMES = "student_id,student_age,student_sex,education_unversity,education_major,education_degree";

		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context)
				throws InterruptedException, IOException {
			HashMap<String, String> qualifierValueMap = new HashMap<>();
			// 循环每个列族
			NavigableMap<byte[], byte[]> kvs = null;
			for (String qualifier : COLUMN_FAMILYS.split(",")) {
				kvs = value.getFamilyMap(Bytes.toBytes(qualifier));
				for (Entry<byte[], byte[]> kv : kvs.entrySet()) {
					if ("student_id".equals(Bytes.toString(kv.getKey()))
							| "student_age".equals(Bytes.toString(kv.getKey()))) {
						qualifierValueMap.put(Bytes.toString(kv.getKey()),
								Bytes.toInt(kv.getValue()) + "");
					} else {
						qualifierValueMap.put(Bytes.toString(kv.getKey()),
								Bytes.toString(kv.getValue()));
					}
				}
			}
			String rowKey = Bytes.toString(value.getRow());
			StringBuilder sb = new StringBuilder();
			sb.append(rowKey.split("\\|")[1]);
			for (String qualifier : QUALIFIER_NAMES.split(",")) {
				sb.append(",");
				sb.append(qualifierValueMap.get(qualifier));
			}
			text.set(sb.toString());
			context.write(text, NullWritable.get());
		}

	}

}
