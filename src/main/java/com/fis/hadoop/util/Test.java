package com.fis.hadoop.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Test {
	static Map<String, Integer> map = new HashMap<String, Integer>();
public static void main(String[] args)  throws Exception {
	File file = new File("C:\\Dais\\workspace\\test");
	fileScan(file);
	for(String key:map.keySet()){
		if(map.get(key)>1){
			System.out.println(key+"---->"+map.get(key));
		}
	}
	
}
	
	
	public static void fileScan(File file)  throws Exception{
		if (file.isDirectory()) {
			if(file.getName().equals("logAnalysis")||file.getName().contains("ngbf-")){
				return;
			}
			File files[] = file.listFiles();
			for (File fil : files) {
				fileScan(fil);	
			}
		}else if (file.isFile()) {
			 String name = file.getName();
			 if(name.contains(".java")){
			 if(map.get(name)!=null){
				 map.put(name, map.get(name)+1);
			 }else{
				 map.put(name, 1);
				 }
			 }
		}

}
	
}
