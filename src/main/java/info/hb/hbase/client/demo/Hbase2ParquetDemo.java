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
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetOutputFormat;

/**
 * 将HBase中的数据表导出Parquet格式
 *
 * 生成Schema对象命令：
 * $ java -jar lib/avro-tools-1.7.7.jar compile schema src/main/resources/avro/student.avsc src/main/java
 *
 * 在CDH5上运行命令：
 * 1、在root用户下操作
 * $ mv apt-hdfs-1.0.0-jar-with-dependencies.jar /var/lib/hadoop-hdfs/
 * 2、在hdfs用户下操作
 * $ su - hdfs
 * $ hadoop jar apt-hdfs-1.0.0-jar-with-dependencies.jar zx.soft.apt.hdfs.hbase.demo.Hbase2ParquetDemo
 * $ hadoop fs -text /user/wanggang/hbase2parquet/part-m-00000.parquet | less
 *
 * 性能测试结果：1亿条数据运行10分钟左右
 *
 * @author wanggang
 *
 */
public class Hbase2ParquetDemo {

	public static void main(String[] args) throws IOException, InterruptedException,
			ClassNotFoundException {
		String outputPath = "/user/wanggang/hbase2parquet";

		// 配置HDFS
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "kafka04,kafka05,kafka06,kafka07,kafka08");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		// 创建作业
		Job job = Job.getInstance(conf);
		job.setJarByClass(Hbase2ParquetDemo.class);
		job.setJobName("Hbase2ParquetDemo");

		// 创建并设置Scan
		Scan scan = new Scan();
		// 默认是1，但是MapReduce作业不适合这个默认值
		scan.setCaching(500);
		// MapReducer作业不能设置True
		scan.setCacheBlocks(Boolean.FALSE);

		// 初始化作业使用的数据表和Mapper
		TableMapReduceUtil.initTableMapperJob("user_info_test", scan, Hbase2ParquetMapper.class,
				null, null, job);
		job.setNumReduceTasks(0);

		// 设置输出格式压缩编码
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, new Path(outputPath));
		AvroParquetOutputFormat.setSchema(job, Student.getClassSchema());
		AvroParquetOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		// 或者下面也可以配置压缩编码
		//		AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		//		AvroParquetOutputFormat.setCompressOutput(job, true);

		// 设置数据块大小为512M
		AvroParquetOutputFormat.setBlockSize(job, 128 * 1024 * 1024);

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new IOException("error with job");
		}
	}

	public static class Hbase2ParquetMapper extends TableMapper<Void, Student> {

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
			context.write(null, student);
		}

	}

}
