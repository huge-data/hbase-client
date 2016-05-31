package info.hb.hbase.client.core;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import zx.soft.common.conn.pool.ConnectionFactory;

/**
 * Hbase连接工厂
 *
 * @author wanggang
 *
 */
class HbaseFactory implements ConnectionFactory<Connection> {

	private static final long serialVersionUID = 4024923894283696465L;

	private final Configuration configuration;

	/**
	 * 构造方法
	 *
	 * @param hadoopConfiguration hbase配置
	 */
	public HbaseFactory(final Configuration hadoopConfiguration) {
		this.configuration = hadoopConfiguration;
	}

	/**
	 * 构造方法
	 *
	 * @param host     zookeeper地址
	 * @param port     zookeeper端口
	 * @param master   hbase主机
	 * @param rootdir  hdfs数据目录
	 */
	public HbaseFactory(final String host, final String port, final String master,
			final String rootdir) {
		this.configuration = new Configuration();
		this.configuration.set("hbase.zookeeper.quorum", host);
		this.configuration.set("hbase.zookeeper.property.clientPort", port);
		/* 下面两个参数不需要 */
		// 新版HBase不需要此参数
		this.configuration.set("hbase.master", master);
		// 默认是/tmp/hbase-username/hbase
		this.configuration.set("hbase.rootdir", rootdir);
	}

	@Override
	public PooledObject<Connection> makeObject() throws Exception {
		Connection connection = this.createConnection();
		return new DefaultPooledObject<>(connection);
	}

	@Override
	public void destroyObject(PooledObject<Connection> p) throws Exception {
		Connection connection = p.getObject();
		if (connection != null) {
			connection.close();
		}
	}

	@Override
	public boolean validateObject(PooledObject<Connection> p) {
		Connection connection = p.getObject();
		if (connection != null) {
			return ((!connection.isAborted()) && (!connection.isClosed()));
		}

		return false;
	}

	@Override
	public void activateObject(PooledObject<Connection> p) throws Exception {
		// TODO
	}

	@Override
	public void passivateObject(PooledObject<Connection> p) throws Exception {
		// TODO
	}

	@Override
	public Connection createConnection() throws Exception {
		Connection connection = org.apache.hadoop.hbase.client.ConnectionFactory
				.createConnection(configuration);
		return connection;
	}

}
