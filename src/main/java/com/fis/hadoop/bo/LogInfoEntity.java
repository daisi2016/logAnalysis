package com.fis.hadoop.bo;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.fis.hadoop.util.SimpleUtil;

public class LogInfoEntity implements Serializable {

	private static final long serialVersionUID = 6687932447139111195L;
	private String rowkey;
	private String content;

 
	public List<String> getFiles() {
		List<String> list = new ArrayList<String>();
		if (content != null && !"".equals(content)) {
			String files[] = content.split(",");
			for (String str : files) {
				if(SimpleUtil.isNotBlank(str)) {
				list.add(str);
				}
			}
		}
		return list;
	}

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
	public static void main(String[] args) throws Exception {
		file1();
		
	}
	private  static void  file1() throws IOException {
		List<String> list =FileUtils.readLines(new File("d:\\0922.txt"));
		//List<String>list = new ArrayList<String>();
		//list.add("\"20170920002258337636980423028#KS_002,KS_058,KS_069,,KS_004,KS_YMT,KS_007,KS_003,KS_077#s0_0001,s0_0002,s0_0004,s0_0003#\"2069179");
		List<String>out = new ArrayList<String>();
		List<String>out_xy = new ArrayList<String>();
		for(String str:list) {
			if(SimpleUtil.isNotBlank(str)) {
				boolean flag =false;
				String[]array = str.split("#");
				String aotomic=array[2];
				if(array[1].contains("KS_002")&&!array[2].contains("s0_0001")) {
					if(SimpleUtil.isNotBlank(aotomic)) {
						aotomic+=",s0_0001";
					}else {
						aotomic+="s0_0001";
					}
					flag=true;
				}
				if((array[1].contains("KS_007")||array[1].contains("KS_YMT"))&&!array[2].contains("s0_0002")) {
					if(SimpleUtil.isNotBlank(aotomic)) {
						aotomic+=",s0_0002";
					}else {
						aotomic+="s0_0002";
					}
					flag=true;
				}
				if((array[1].contains("KS_004")||array[1].contains("KS_032")||array[1].contains("KS_038"))&&!array[2].contains("s0_0003")) {
					if(SimpleUtil.isNotBlank(aotomic)) {
						aotomic+=",s0_0003";
					}else {
						aotomic+="s0_0003";
					}
					flag=true;
				}
				if((array[1].contains("KS_003"))&&!array[2].contains("s0_0004")) {
					if(SimpleUtil.isNotBlank(aotomic)) {
						aotomic+=",s0_0004";
					}else {
						aotomic+="s0_0004";
					}
					flag=true;
				}
				if(flag) {
					String seria = array[0];
					String accptId = array[3];
					seria=seria.replace("\"", "");
					accptId=accptId.replace("\"", "");
					String updateChoose = "UPDATE SCOS.SCOS_BUSINESS_CHOOSE SET CHOSEN_BUSINESS = '"+aotomic+"' WHERE BUSINESS_ACCEPT_ID = "+accptId+";";
					String updateDetail = "UPDATE SCOS.SCOS_BUS_SCHEDULE_DETAIL SET AUDIT_STATUS = 'TS', ARCHIVE_STATUS = '0',IMAGE_ARCHIVE_STATUS='' WHERE BUS_ID='"+accptId+"';";
					out.add(updateChoose);
					out.add(updateDetail);
					out_xy.add(seria);
				}
				
			}
		} 
		
		FileUtils.writeLines(new File("C:\\Users\\hp\\Desktop\\20170922.sql"), out,true);
		FileUtils.writeLines(new File("C:\\Users\\hp\\Desktop\\20170922_seria_to_xinyi.sql"), out_xy,true);
	}
	 
}
