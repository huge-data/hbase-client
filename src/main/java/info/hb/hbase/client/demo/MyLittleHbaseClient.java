package info.hb.hbase.client.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase官网示例：https://archive.cloudera.com/cdh5/cdh/5/hbase/apidocs/org/apache/hadoop/hbase/client/package-summary.html
 *
 * 该类执行了Put, Get和Scan操作
 *
 * @author wanggang
 *
 */
public class MyLittleHbaseClient {

	public static void main(String[] args) throws IOException {
		// 该配置直接读取CLASSPATH下面的hbase-site.xml、hbase-default.xml文件
		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);) {

			// 获取表名为myLittleHBaseTable的数据表对象
			try (Table table = connection.getTable(TableName.valueOf("myLittleHBaseTable"));) {
				// 通过行键初始化Put对象
				Put p = new Put(Bytes.toBytes("myLittleRow"));
				// 通过列族（创建表的时候指定）、列标识（任意值）、值添加列数据
				p.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"),
						Bytes.toBytes("Some Value"));
				// 提交数据到HBase
				table.put(p);

				// 通过行键初始化Get对象
				Get g = new Get(Bytes.toBytes("myLittleRow"));
				Result r = table.get(g);
				byte[] value = r.getValue(Bytes.toBytes("myLittleFamily"),
						Bytes.toBytes("someQualifier"));
				String valueStr = Bytes.toString(value);
				System.out.println("GET: " + valueStr);

				// 扫描所有数据
				Scan s = new Scan();
				s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
				ResultScanner scanner = table.getScanner(s);
				try {
					for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
						System.out.println("Found row: " + rr);
					}

					// 另一种循环
					//					for (Result rr : scanner) {
					//						System.out.println("Found row: " + rr);
					//					}
				} finally {
					scanner.close();
				}

				// Close your table and cluster connection.
			}
		}
	}

}
