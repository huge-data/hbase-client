package info.hb.hbase.client.demo;

import info.hb.hbase.client.core.HbasePool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import zx.soft.common.conn.pool.PoolConfig;

/**
 * HBase连接池示例：包含Table数据写入、批量异步写入、数据扫描
 *
 * 在CDH5上运行命令：
 * 1、在root用户下操作
 * $ cp apt-hdfs-1.0.0-jar-with-dependencies.jar /var/lib/hadoop-hdfs/
 * 2、在hdfs用户下操作
 * $ su - hdfs
 * $ hadoop jar apt-hdfs-1.0.0-jar-with-dependencies.jar zx.soft.apt.hdfs.hbase.demo.HbasePoolDemo
 *
 * @author wanggang
 *
 */
public class HbasePoolDemo {

	public static void main(String[] args) throws IOException {
		/* 连接池配置 */
		PoolConfig config = new PoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000);
		config.setTestOnBorrow(true);

		/* Hbase配置 */
		Configuration hbaseConfig = new Configuration();
		hbaseConfig.set("hbase.zookeeper.quorum", "kafka04,kafka05,kafka06,kafka07,kafka08");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

		/* 初始化连接池 */
		HbasePool pool = new HbasePool(config, hbaseConfig);

		/* 从连接池中获取对象 */
		Connection conn = pool.getConnection();

		int count = Integer.parseInt(args[0]);
		int batch = Integer.parseInt(args[1]);
		// 注意：HBase插入很小的整数（32以下吧）时，由于存储的是字节，在Hue界面上查询时不显示字节值，而不是错误
		// 批量异步写入
		try (BufferedMutator bm = conn.getBufferedMutator(TableName.valueOf("user_info_test"));) {
			for (int j = 0; j < count / 2; j++) {
				long start = System.currentTimeMillis();
				List<Put> mutations = new ArrayList<>();
				// 批量写入10万的话，平均4秒可写入一次，批量写入100万会导致内存泄漏
				for (int i = 0; i < batch; i++) {
					Put put = new Put(Bytes.toBytes("uid|" + (i + j * batch + 1_000_000_000)));
					put.addColumn(Bytes.toBytes("id_col"), Bytes.toBytes("student_id"),
							Bytes.toBytes((i + 100_000)));
					put.addColumn(Bytes.toBytes("base_col"), Bytes.toBytes("student_age"),
							Bytes.toBytes((i % 5 + 100)));
					put.addColumn(Bytes.toBytes("base_col"), Bytes.toBytes("student_sex"),
							Bytes.toBytes((i % 2 == 0) ? "男" : "女"));
					put.addColumn(Bytes.toBytes("education_col"),
							Bytes.toBytes("education_unversity"), Bytes.toBytes("合肥工业大学" + i));
					put.addColumn(Bytes.toBytes("education_col"), Bytes.toBytes("education_major"),
							Bytes.toBytes("美容专业" + i));
					put.addColumn(Bytes.toBytes("education_col"),
							Bytes.toBytes("education_degree"), Bytes.toBytes("博士后" + i));
					mutations.add(put);
				}
				bm.mutate(mutations);
				long end = System.currentTimeMillis();
				System.err.println("批量方式，第" + j + "次，写入时间：" + (end - start) + "毫秒.");
				mutations.clear();
			}
		}
		System.out.println("批量方式写入数据完成！");

		/** 注意 **/
		// HBase这样循环批量写入数据，会报如下错误，但是执行完成，无数据丢失：
		// 16/01/10 17:07:12 INFO client.AsyncProcess: #3, table=user_info_test, attempt=13/31 failed=1331ops,
		// last exception: org.apache.hadoop.hbase.NotServingRegionException: org.apache.hadoop.hbase.NotServingRegionException:
		// Region user_info_test,uid|1036624018,1452416588708.2c96c2946466724894bb60563437c9f3.
		// is not online on kafka07,60020,1452170616300

		// 获取表，用完需要关闭
		try (Table table = conn.getTable(TableName.valueOf("user_info_test"));) {
			// 插入数据
			for (int j = count / 2; j < count; j++) {
				long start = System.currentTimeMillis();
				// 批量写入10万的话，平均4秒可写入一次，批量写入100万会导致内存泄漏
				List<Put> puts = new ArrayList<>();
				for (int i = 0; i < batch; i++) {
					Put put = new Put(Bytes.toBytes("uid|" + (i + j * batch + 1_000_000_000)));
					put.addColumn(Bytes.toBytes("id_col"), Bytes.toBytes("student_id"),
							Bytes.toBytes((i + 100_000)));
					put.addColumn(Bytes.toBytes("base_col"), Bytes.toBytes("student_age"),
							Bytes.toBytes((i % 5 + 100)));
					put.addColumn(Bytes.toBytes("base_col"), Bytes.toBytes("student_sex"),
							Bytes.toBytes((i % 2 == 0) ? "男" : "女"));
					put.addColumn(Bytes.toBytes("education_col"),
							Bytes.toBytes("education_unversity"), Bytes.toBytes("合肥工业大学" + i));
					put.addColumn(Bytes.toBytes("education_col"), Bytes.toBytes("education_major"),
							Bytes.toBytes("美容专业" + i));
					put.addColumn(Bytes.toBytes("education_col"),
							Bytes.toBytes("education_degree"), Bytes.toBytes("博士后" + i));
					puts.add(put);
				}
				table.put(puts);
				long end = System.currentTimeMillis();
				System.err.println("普通方式，第" + j + "次，写入时间：" + (end - start) + "毫秒.");
				puts.clear();
			}
			System.out.println("普通方式写入数据完成！");

			// 扫描所有数据
			/*
			Scan s = new Scan();
			s.addColumn(Bytes.toBytes("col1"), Bytes.toBytes("qualifier"));
			ResultScanner scanner = table.getScanner(s);
			try {
				int i = 0;
				for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
					System.out.println("第" + ++i + " 行: " + rr);
				}

				// 另一种循环
				//					for (Result rr : scanner) {
				//						System.out.println("Found row: " + rr);
				//					}
			} finally {
				scanner.close();
			}
			*/
		}

		// 返回连接资源
		pool.returnConnection(conn);

		/* 关闭连接池 */
		pool.close();
	}

}
