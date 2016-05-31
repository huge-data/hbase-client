package info.hb.hbase.client.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import zx.soft.common.conn.pool.ConnectionPool;
import zx.soft.common.conn.pool.PoolBase;
import zx.soft.common.conn.pool.PoolConfig;

/**
 * Hbase连接池
 *
 * @author wanggang
 *
 */
public class HbasePool extends PoolBase<Connection> implements ConnectionPool<Connection> {

	private static final long serialVersionUID = -9126420905798370243L;

	/**
	 * 默认构造方法
	 */
	public HbasePool() {
		this("localhost", "2181");
	}

	/**
	 * 构造方法
	 *
	 * @param host 地址
	 * @param port 端口
	 */
	public HbasePool(final String host, final String port) {
		this(new PoolConfig(), host, port, null, null);
	}

	/**
	 * 构造方法
	 *
	 * @param host 地址
	 * @param port 端口
	 * @param master hbase主机
	 * @param rootdir hdfs目录
	 */
	public HbasePool(final String host, final String port, final String master,
			final String rootdir) {
		this(new PoolConfig(), host, port, master, rootdir);
	}

	/**
	 * 构造方法
	 *
	 * @param hadoopConfiguration hbase配置
	 */
	public HbasePool(final Configuration hadoopConfiguration) {
		this(new PoolConfig(), hadoopConfiguration);
	}

	/**
	 * 构造方法
	 *
	 * @param poolConfig 池配置
	 * @param host 地址
	 * @param port 端口
	 */
	public HbasePool(final PoolConfig poolConfig, final String host, final String port) {
		this(poolConfig, host, port, null, null);
	}

	/**
	 * 构造方法
	 *
	 * @param poolConfig 池配置
	 * @param hadoopConfiguration hbase配置
	 */
	public HbasePool(final PoolConfig poolConfig, final Configuration hadoopConfiguration) {
		super(poolConfig, new HbaseFactory(hadoopConfiguration));
	}

	/**
	 * 构造方法
	 *
	 * @param poolConfig 池配置
	 * @param host 地址
	 * @param port 端口
	 * @param master hbase主机
	 * @param rootdir hdfs目录
	 */
	public HbasePool(final PoolConfig poolConfig, final String host, final String port,
			final String master, final String rootdir) {
		super(poolConfig, new HbaseFactory(host, port, master, rootdir));
	}

	@Override
	public Connection getConnection() {
		return super.getResource();
	}

	@Override
	public void returnConnection(Connection conn) {
		super.returnResource(conn);
	}

	@Override
	public void invalidateConnection(Connection conn) {
		super.invalidateResource(conn);
	}

}
