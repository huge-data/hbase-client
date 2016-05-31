package info.hb.hbase.client.demo;

import info.hb.hbase.client.core.HbasePool;
import info.hb.hbase.client.utils.Cdh5Config;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

import zx.soft.common.conn.pool.PoolConfig;

public class HbaseAdminDemo {

	public static void main(String[] args) throws IOException {
		/* 连接池配置 */
		PoolConfig config = new PoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000);
		config.setTestOnBorrow(true);

		/* Hbase配置 */
		Configuration hbaseConfig = Cdh5Config.getHbaseConf();

		/* 初始化连接池 */
		HbasePool pool = new HbasePool(config, hbaseConfig);

		/* 从连接池中获取对象 */
		Connection conn = pool.getConnection();

		/* 获取Admin对象 */
		try (Admin admin = conn.getAdmin();) {
			// 1、创建表，默认版本数量是多个
			String tableName = "user_info_test";
			String[] columnFamilys = { "id_col", "base_col", "education_col" };
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			for (String columnFamily : columnFamilys) {
				tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			}
			admin.createTable(tableDescriptor);
			System.out.println("Create table successful!");

			// 2、判断数据表是否存在
			//		booleansExisted = admin.tableExists(TableName.valueOf(tableName));
			//		boolean isExisted = admin.isTableAvailable(TableName.valueOf(tableName));
			//		System.out.println("Table isExisted: " + isExisted);

			// 3、删除表
			//		admin.disableTable(TableName.valueOf(tableName));
			//		admin.deleteTable(TableName.valueOf(tableName));
			//		System.out.println("Table deleted: "
			//				+ !admin.isTableAvailable(TableName.valueOf(tableName)));
		}

		/* 返回连接资源 */
		pool.returnConnection(conn);

		/* 关闭连接池 */
		pool.close();
	}

	public static void createTable(String tableName, String[] columnFamilys, short regionCount,
			long regionMaxSize, Admin admin) throws IOException {
		System.out.println("Creating Table: " + tableName);
		// 创建数据表描述器
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		// 设置并添加列族描述器
		for (String columnFamily : columnFamilys) {
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
			// 数据压缩算法
			columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
			// 块大小
			columnDescriptor.setBlocksize(128 * 1024);
			// 布隆过滤器类型
			columnDescriptor.setBloomFilterType(BloomType.ROW);
			tableDescriptor.addFamily(columnDescriptor);
		}
		// 设置最大文件大小
		tableDescriptor.setMaxFileSize(regionMaxSize);
		// 根据最大常量值设置分区策略
		tableDescriptor.setValue(HTableDescriptor.SPLIT_POLICY,
				ConstantSizeRegionSplitPolicy.class.getName());

		// 分区数和范围
		regionCount = (short) Math.abs(regionCount);
		int regionRange = Short.MAX_VALUE / regionCount;
		// 分区key
		int counter = 0;
		byte[][] splitKeys = new byte[regionCount][];
		for (int i = 0; i < splitKeys.length; i++) {
			counter = counter + regionRange;
			String key = StringUtils.leftPad(Integer.toString(counter), 5, '0');
			splitKeys[i] = Bytes.toBytes(key);
			System.out.println(" - Split: " + i + " '" + key + "'");
		}

		admin.createTable(tableDescriptor, splitKeys);
	}

}
