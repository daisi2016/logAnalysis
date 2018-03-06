package com.fis.hadoop.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsFileRead {
	public static void main(String[] args,String...str) throws Exception {
		String fileName="test.log";
		String url="hdfs://10.243.140.139:9000/home/hadoop_admin/repository/"+fileName;
		Configuration config =  new Configuration();
		FileSystem fs = FileSystem.get(URI.create(url),config);
		//InputStream in = fs.open(new Path(url));
		FSDataInputStream in=fs.open(new Path(url));
		File file = new File("C:\\log\\test\\");
		if(!file.exists()){
			file.mkdirs();
		}
		OutputStream out = new FileOutputStream("C:\\log\\test\\"+fileName);
		//in.seek(0);
		IOUtils.copyBytes(in,out, 4096,true);
		
		System.out.println(in.getPos());
	
		
	//	IOUtils.copyBytes(in,System.out, 4096,true);
		
		IOUtils.closeStream(in);
		IOUtils.closeStream(out);
		
	}
	

}
