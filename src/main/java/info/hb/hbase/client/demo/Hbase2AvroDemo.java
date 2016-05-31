package info.hb.hbase.client.demo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
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
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;

/**
 * 将HBase中的数据表导出Avro格式
 *
 * 生成Schema对象命令：
 * $ java -jar lib/avro-tools-1.7.7.jar compile schema src/main/resources/avro/student.avsc src/main/java
 *
 * 在CDH5上运行命令：
 * 1、在root用户下操作
 * $ mv apt-hdfs-1.0.0-jar-with-dependencies.jar /var/lib/hadoop-hdfs/
 * 2、在hdfs用户下操作
 * $ su - hdfs
 * $ hadoop jar apt-hdfs-1.0.0-jar-with-dependencies.jar zx.soft.apt.hdfs.hbase.demo.Hbase2AvroDemo
 * $ hadoop fs -text /user/wanggang/hbase2avro/part-m-00000.avro | less
 *
 * 性能测试结果：1亿条数据运行10分钟左右
 *
 * 运行错误：Error: Found interface org.apache.hadoop.mapreduce.TaskAttemptContext, but class was expected
 * 原因分析：avro-mapred版本不兼容
 * 解决方案：在依赖avro-mapred时添加classifier为hadoop2
 * 参考资料：http://stackoverflow.com/questions/19089557/how-does-cloudera-cdh4-work-with-avro
 *
 * @author wanggang
 *
 */
public class Hbase2AvroDemo {

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		String outputPath = "/user/wanggang/hbase2avro";

		// 配置HDFS
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "kafka04,kafka05,kafka06,kafka07,kafka08");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		// 创建作业
		Job job = Job.getInstance(conf);
		job.setJarByClass(Hbase2AvroDemo.class);
		job.setJobName("Hbase2AvroDemo");

		// 创建并设置Scan
		Scan scan = new Scan();
		// 默认是1，但是MapReduce作业不适合这个默认值
		scan.setCaching(500);
		// MapReducer作业不能设置True
		scan.setCacheBlocks(Boolean.FALSE);

		// 初始化作业使用的数据表和Mapper
		TableMapReduceUtil.initTableMapperJob("user_info_test", scan, Hbase2AvroMapper.class, null,
				null, job);
		job.setNumReduceTasks(0);

		// 设置输出格式和编码
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		AvroJob.setOutputKeySchema(job, Student.getClassSchema());
		AvroKeyOutputFormat.setOutputPath(job, new Path(outputPath));
		AvroKeyOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new IOException("error with job");
		}
	}

	public static class Hbase2AvroMapper extends TableMapper<AvroKey<Student>, NullWritable> {

		AvroKey<Student> avroKey = new AvroKey<>();
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
			//  用行键代替qualifierValueMap.get("student_id"))，保证每条数据唯一
			Student student = new Student(
			// student_id
					Integer.parseInt(rowKey.split("\\|")[1]),
					// student_base_info
					new StudentBaseInfo(Integer.parseInt(qualifierValueMap.get("student_age")),
							qualifierValueMap.get("student_sex")),
					// student_education_info
					new StudentEducationInfo(qualifierValueMap.get("education_unversity"),
							qualifierValueMap.get("education_major"), qualifierValueMap
									.get("education_degree")));
			avroKey.datum(student);
			context.write(avroKey, NullWritable.get());
		}

	}

}
