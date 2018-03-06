package com.fis.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**文件上传
 * @author si.dai
 *
 */
public class HdfsFileUpload  {
	Log log = LogFactory.getLog(HdfsFileUpload.class);
	private Map<String,Set<String>>map = new HashMap<String,Set<String>> ();
	private final String urlPattern="hdfs://10.243.140.139:9000/home/hadoop_admin/repository/dfs";
	
	public void CopyAndAppendFiles(File file, String fixName)  throws Exception{
		if (file.isDirectory()) {
			File files[] = file.listFiles();
			for (File fil : files) {
				if (fil.isDirectory() && (fil.getName().equals("broker") || fil.getName().equals("scos") || fil.getName().equals("tpsp"))) {
					CopyAndAppendFiles(fil, fil.getName());
				}else{
					CopyAndAppendFiles(fil, fixName);	
				}
			}
		} else if (file.isFile()) {
			Date date = new Date(file.lastModified());
			SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
			// 对应hbase里的row key
			String key =  sf.format(date)+fixName;
			String nameArray[] = (file.getName()).split("\\.");
			if("log".equalsIgnoreCase(nameArray[nameArray.length - 1])){
			String fsFileName = "/"+sf.format(date) + "/"+ fixName+"/"+ nameArray[0] + "." + nameArray[nameArray.length - 1];
			if (map.get(key) != null) {
				map.get(key).add(fsFileName);
			} else {
				Set<String> set = new HashSet<String>();
				set.add(fsFileName);
				map.put(key, set);
			}
				fileUpload(file, fsFileName);
			}
			 
		}

	}
	
	private void fileUpload(final File sourceFilepath,final String fsFileName)  throws Exception {
		InputStream in =  new BufferedInputStream(new FileInputStream(sourceFilepath));
		System.setProperty("HADOOP_USER_NAME", "root");
		
		String url=urlPattern+fsFileName;
		Configuration config =  new Configuration();
		config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		FileSystem fs = FileSystem.get(URI.create(url),config);
		OutputStream out=null;
		if (!fs.exists(new Path(url))) {
			out = fs.create(new Path(url), new Progressable() {
				public void progress() {
					log.info("上传 :" + sourceFilepath + "--to->" + fsFileName);
				}
			});
		} else {
			// out = fs.append(new Path(url));
			out = fs.append(new Path(url), 2048, new Progressable() {
				public void progress() {
					log.info("追加 :" + sourceFilepath + "--to->" + fsFileName);

				}
			});
		}
		IOUtils.copyBytes(in, out, 4096,true);
		
		IOUtils.closeStream(in);
		IOUtils.closeStream(out);
		
	}
	
	public Map<String, Set<String>> getMap() {
		return map;
	}

	public void setMap(Map<String, Set<String>> map) {
		this.map = map;
	}

	
	
	
 
	/*public static void main(String[] args) throws Exception {
		InputStream in =  new BufferedInputStream(new FileInputStream("C:\\log\\160-tpsp\\ngbf-cpack.log"));
		System.setProperty("HADOOP_USER_NAME", "root");
		
		String fileName="test.log";
		String url="hdfs://10.243.140.139:9000/home/hadoop_admin/repository/"+fileName;
		Configuration config =  new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),config);
		OutputStream out = fs.create(new Path(url), new Progressable() {
			@Override
			public void progress() {
			System.out.print(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096,true);
		
	}*/

}
