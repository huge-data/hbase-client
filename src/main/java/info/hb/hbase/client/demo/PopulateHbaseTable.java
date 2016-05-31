package info.hb.hbase.client.demo;

import info.hb.hbase.client.core.NumMapperInputFormat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 加载Hbase中某张数据表到HDFS中
 *
 * 未测试
 *
 * @author wanggang
 *
 */
public class PopulateHbaseTable {

	public static Logger logger = LoggerFactory.getLogger(PopulateHbaseTable.class);

	public static String TABLE_NAME = "custom.table.name";
	public static String COLUMN_FAMILY = "custom.column.family";
	public static String RUN_ID = "custom.runid";
	public static String NUMBER_OF_RECORDS = "custom.number.of.records";

	public static void main(String[] args) throws Exception {

		String outputPath = "/user/hdfs/output_populate";
		String tableName = "apt_test";
		String columnFamily = "col1";
		String runID = "populate-hbase-table";

		// 创建作业
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://kafka04:8020");
		Job job = Job.getInstance(conf);
		job.setJarByClass(PopulateHbaseTable.class);
		job.setJobName("PopulateTable: " + runID);
		job.getConfiguration().set(NUMBER_OF_RECORDS, "100");
		job.getConfiguration().set(TABLE_NAME, tableName);
		job.getConfiguration().set(COLUMN_FAMILY, columnFamily);
		job.getConfiguration().set(RUN_ID, runID);
		job.setInputFormatClass(NumMapperInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		NumMapperInputFormat.setNumMapTasks(job.getConfiguration(), 4);

		// 创建HBase配置和Table
		HBaseConfiguration.addHbaseResources(job.getConfiguration());
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf(tableName));

		// 自动配置分区和Reducer
		HFileOutputFormat2.configureIncrementalLoadMap(job, table);

		// 定义Mapper和Reducer
		job.setMapperClass(CustomMapper.class);
		job.setNumReduceTasks(0);

		// 定义Mapper输出Key和Value格式
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);

		// 退出作业
		job.waitForCompletion(true);

		// 获取文件系统
		FileSystem hdfs = FileSystem.get(config);

		// 修改HBase对输出目录有写权限
		changePermissionR(outputPath, hdfs);

		// 将HFileOutputFormat2格式的输出加载到数据表中
		LoadIncrementalHFiles load = new LoadIncrementalHFiles(config);
		load.doBulkLoad(new Path(outputPath), (HTable) table);
	}

	private static void changePermissionR(String output, FileSystem hdfs)
			throws FileNotFoundException, IOException {
		logger.info("Change privs: {}.", output);
		FileStatus[] fsList = hdfs.listStatus(new Path(output));
		hdfs.setPermission(new Path(output), FsPermission.createImmutable(Short.valueOf("777", 8)));
		for (FileStatus fs : fsList) {
			if (fs.isDirectory()) {
				changePermissionR(fs.getPath().toString(), hdfs);
			} else {
				logger.info("Change privs: {}.", fs.getPath());
				hdfs.setPermission(fs.getPath(),
						FsPermission.createImmutable(Short.valueOf("777", 8)));
			}
		}
	}

	public static class CustomMapper extends
			Mapper<NullWritable, NullWritable, ImmutableBytesWritable, KeyValue> {

		ImmutableBytesWritable hKey = new ImmutableBytesWritable();
		KeyValue kv;

		Pattern p = Pattern.compile("\\|");
		byte[] columnFamily;

		String runID;
		int taskId;
		int numberOfRecords;

		@Override
		public void setup(Context context) {
			System.out.println("starting setup");

			columnFamily = Bytes.toBytes(context.getConfiguration().get(COLUMN_FAMILY));
			runID = context.getConfiguration().get(RUN_ID);
			taskId = context.getTaskAttemptID().getTaskID().getId();
			numberOfRecords = context.getConfiguration().getInt(NUMBER_OF_RECORDS, 1000)
					/ context.getConfiguration().getInt("nmapinputformat.num.maps", 1);

			System.out.println("finished setup");
		}

		Random r = new Random();

		@Override
		public void map(NullWritable key, NullWritable value, Context context) throws IOException,
				InterruptedException {
			int counter = 0;

			System.out.println("starting mapper");
			System.out.println();
			for (int i = 0; i < numberOfRecords; i++) {
				String keyRoot = StringUtils.leftPad(Integer.toString(r.nextInt(Short.MAX_VALUE)),
						5, '0');

				if (i % 1000 == 0) {
					System.out.print(".");
				}

				for (int j = 0; j < 10; j++) {
					hKey.set(Bytes.toBytes(keyRoot + "|" + runID + "|" + taskId));
					kv = new KeyValue(hKey.get(), columnFamily, Bytes.toBytes("C" + j),
							Bytes.toBytes("counter:" + counter++));
					context.write(hKey, kv);
				}
			}

			System.out.println("finished mapper");
		}

	}

}
