package com.fis.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.fis.hadoop.hdfs.HdfsFileUpload;


/**文件上传并结构化数据关键信息
 * @author si.dai
 *
 */
public class LogFilesFormat {
	private static final String tableName = "log_info_entity";
	private static final String columns = "content";
	private static final String column = "filename";
	
	public static void logfileFomate(String inputDir){
		 Map<String,Set<String>>map =null;
		try {
			HdfsFileUpload upload = new HdfsFileUpload();
			upload.CopyAndAppendFiles(new File(inputDir), "");
			map = upload.getMap();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(map!=null&&!map.isEmpty()){
			try {
				HTableDao.createTable("", tableName, columns, false);
				String rowKeys[]= new String[map.keySet().size()];;
				String columArray[]= new String[map.keySet().size()];
				String values[]= new String[map.keySet().size()];
				int i=0;
				for(String key:map.keySet()){
					rowKeys[i]=key;
					columArray[i]=column;
					Set<String>value= map.get(key);
					String filepaths ="";
					for(String val:value){
						filepaths += val+",";
					}
					values[i]=filepaths;
					i++;
				}				
				HTableDao.insertInfo(tableName, rowKeys, columns, columArray, values);
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		LogFilesFormat.logfileFomate("C:\\log\\2016120");
		//LogFilesFormat.logfileFomate("C:\\log\\test");
		//String str="ownerSecurities#ownerName#ownerAccount#ownerHa#ownerSa#associateSecurities#associateName#associateAccount#associateHa#associateSa#htShareholder#htShares#htAssociate#htRelationship#sfyzxdr#yzxdr#sfdjg#sssf#listedCompanyName#listedCompanyCode#cgbl#glgx#creditContract#companyContract#creditHolderAcc#creditHa#creditSa#sfcyltxsg#holderName#holderCode#holderShare#holderNature#liftedDate#";
		//String [] array = str.split("#");
		/*for(String arr:array) {
			System.out.println("<"+arr+"></"+arr+">");
		}*/
	}

}
