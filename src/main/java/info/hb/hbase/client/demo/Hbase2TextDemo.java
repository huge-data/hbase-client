package info.hb.hbase.client.demo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * 将HBase中的数据表导出逗号分割的Text文件
 *
 * 在CDH5上运行命令：
 * 1、在root用户下操作
 * $ mv apt-hdfs-1.0.0-jar-with-dependencies.jar /var/lib/hadoop-hdfs/
 * 2、在hdfs用户下操作
 * $ su - hdfs
 * $ hadoop jar apt-hdfs-1.0.0-jar-with-dependencies.jar zx.soft.apt.hdfs.hbase.demo.Hbase2TextDemo
 * $ hadoop fs -text /user/wanggang/hbase2text/part-m-00000 | less
 *
 * 性能测试结果：1亿条数据运行10分钟左右
 *
 * @author wanggang
 *
 */
public class Hbase2TextDemo {

	public static final String OUTPUT_PATH = "output.path";

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		String outputPath = "/user/wanggang/hbase2text";

		// 配置HDFS
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "kafka04,kafka05,kafka06,kafka07,kafka08");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set(OUTPUT_PATH, outputPath);

		// 创建作业
		Job job = Job.getInstance(conf);
		job.setJarByClass(Hbase2TextDemo.class);
		job.setJobName("Hbase2TextDemo");

		// 创建并设置Scan
		Scan scan = new Scan();
		// 默认是1，但是MapReduce作业不适合这个默认值
		scan.setCaching(500);
		// MapReducer作业不能设置True
		scan.setCacheBlocks(Boolean.FALSE);

		// 节点上的Mapper运行HBase处理作业时需要
		//		TableMapReduceUtil.addDependencyJars(job);
		//		TableMapReduceUtil.addHBaseDependencyJars(conf);

		// 初始化作业使用的数据表和Mapper
		TableMapReduceUtil.initTableMapperJob("user_info_test", scan, Hbase2TextMapper.class, null,
				null, job);
		job.setNumReduceTasks(0);

		// 作业输出格式
		job.setOutputFormatClass(NullOutputFormat.class);

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new IOException("error with job");
		}
	}

	private static class Hbase2TextMapper extends TableMapper<Text, Text> {

		FileSystem fs;
		BufferedWriter writer;
		static final String COLUMN_FAMILYS = "id_col,base_col,education_col";
		static final String QUALIFIER_NAMES = "student_id,student_age,student_sex,education_unversity,education_major,education_degree";
		int taskId;

		@Override
		public void setup(Context context) throws TableNotFoundException, IOException {
			fs = FileSystem.get(context.getConfiguration());
			taskId = context.getTaskAttemptID().getTaskID().getId();
			String outputPath = context.getConfiguration().get(OUTPUT_PATH);
			OutputStream outputStream = fs.create(
					new Path(outputPath + "/part-m-"
							+ StringUtils.leftPad(Integer.toString(taskId), 5, "0")), true);
			outputStream = new GZIPOutputStream(outputStream);
			writer = new BufferedWriter(new OutputStreamWriter(outputStream));
		}

		@Override
		public void cleanup(Context context) throws IOException {
			writer.close();
			fs.close();
		}

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
			StringBuilder sb = new StringBuilder();
			// 注意竖线是正则表达式约定符号，必须转义才能作为一般符号
			sb.append(Bytes.toString(value.getRow()).split("\\|")[1]);
			for (String qualifier : QUALIFIER_NAMES.split(",")) {
				sb.append(",");
				sb.append(qualifierValueMap.get(qualifier));
			}
			writer.write(sb.toString());
			writer.newLine();
		}

	}

}
