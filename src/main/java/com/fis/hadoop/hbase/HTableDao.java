package com.fis.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.fis.hadoop.bo.LogInfoEntity;

/**hbase表操作
 * @author si.dai
 *
 */
public class HTableDao {
	private static	Log log = LogFactory.getLog(HTableDao.class);
	private static Configuration conf = null;
	/**
	 * 初始化配置
	 */
	static {
		conf = HBaseConfiguration.create();
	}

	/**
	 * 创建表操作
	 * 
	 * @throws IOException
	 */
	public static void createTable(String namespace, String tablename, String cfs, boolean isdeleteWhenExist) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.getConnection();
		/*NamespaceDescriptor desc;
		try {
			desc = admin.getNamespaceDescriptor(namespace);
		} catch (Exception e) {
			if (e instanceof NamespaceNotFoundException) {
				admin.createNamespace(NamespaceDescriptor.create(namespace).build());
			}
			e.printStackTrace();
		}
		if (!(namespace == null || namespace.equals(""))) {
			tablename = namespace + ":" + tablename;
		}*/
		if (admin.tableExists(tablename)) {
			System.out.println(tablename + "表已经存在！");
			if (isdeleteWhenExist) {
				admin.disableTable(tablename);
				admin.deleteTable(tablename);
				System.out.println(tablename + "表删除成功！");
			} else {
				return;
			}
		}
		HTableDescriptor tableDesc = null;
		tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
		tableDesc.setDurability(Durability.SYNC_WAL);
		/*
		 * 1.由于HBase的数据是先写入内存，数据累计达到内存阀值时才往磁盘中flush数据，所以，如果在数据还没有flush进硬盘时，
		 * regionserver down掉了，内存中的数据将丢失。
		 * 要想解决这个场景的问题就需要用到WAL（Write-Ahead-Log），tableDesc
		 * .setDurability(Durability. SYNC_WAL ) 就是设置写WAL日志的级别，
		 * 示例中设置的是同步写WAL，该方式安全性较高，但无疑会一定程度影响性能，请根据具体场景选择使用； 2.setDurability
		 * (Durability d)方法可以在相关的三个对象中使用，分别是：HTableDescriptor， Delete，
		 * Put（其中Delete和Put的该方法都是继承自父类org.apache.hadoop.hbase.client.Mutation） 。
		 * 分别针对表、插入操作、删除操作设定WAL日志写入级别。需要注意的是，
		 * Delete和Put并不会继承Table的Durability级别（已实测验证） 。
		 * Durability是一个枚举变量，可选值参见4.2节。如果不通过该方法指定WAL日志级别，则为 默认 USE_DEFAULT 级别
		 */

		// 创建列镞
		HColumnDescriptor hcd = new HColumnDescriptor(cfs);
		tableDesc.addFamily(hcd);
		admin.createTable(tableDesc);
		System.out.println(tablename + "表创建成功！");
		admin.close();
	}
	public static void insertInfo(String tablename,String rowKey[],String colums,String colum[],String value[]) throws IOException {

		HTable table = new HTable(conf, tablename);
		int i=0;
		for(String str:rowKey){
		Put put = new Put(Bytes.toBytes(str));
		put.add(Bytes.toBytes(colums), Bytes.toBytes(colum[i]), Bytes.toBytes(value[i]));
		table.put(put);
		i++;
		}
		table.close();
	}
	
	public static List<LogInfoEntity>query(String tablename,String rowKeyFix) throws IOException{
		List<LogInfoEntity> list = new ArrayList<LogInfoEntity>();
		HTable table = new HTable(conf, tablename);	
		Scan s  = new Scan();
		s.setFilter(new PrefixFilter(rowKeyFix.getBytes()));
		ResultScanner result = table.getScanner(s);
		for(Result rs:result){
			for (Cell cell : rs.rawCells()) {
				LogInfoEntity entity = new LogInfoEntity();
				entity.setRowkey(Bytes.toString(rs.getRow()));
				entity.setContent(Bytes.toString(CellUtil.cloneValue(cell)));
				list.add(entity);
				log.debug(
				"--------Rowkey : " + Bytes.toString(rs.getRow()) +
				"   Familiy:Quilifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
				"   Value : " + Bytes.toString(CellUtil.cloneValue(cell))
				+ "--------------");
			}
		}
		table.close();
	return list;	
	}
	
	public static LogInfoEntity get(String tablename,String rowKey) throws IOException {
		HTable table = new HTable(conf, tablename);
		Get get = new Get(Bytes.toBytes(rowKey));
		Result r = table.get(get);
		LogInfoEntity entity = new LogInfoEntity();
		for (Cell cell : r.rawCells()) {
			entity.setRowkey(Bytes.toString(r.getRow()));
			entity.setContent(Bytes.toString(CellUtil.cloneValue(cell)));
		}
		table.close();
		
		return entity;
	}
	
	
	public static void main(String[] args) throws IOException {
		
	/*	File file = new File("C:\\Users\\si.dai\\Desktop\\0127\\0127\\160\\tpsp.log");
		
		Date date = new Date(file.lastModified());
		SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHHmm");
		
		System.out.println(sf.format(date));*/
		
		
		query("log_info_entity", "20160120");
	}
 
}
