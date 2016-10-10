package java_test;

import java.util.HashMap;
import java.util.Map;

import com.csvreader.CsvReader;

/**
 * 
 * out:
 * 
 * 
 *  drop table if exists ods.S02_ARTISAN_AGREEMENT;
	CREATE EXTERNAL TABLE ods.S02_ARTISAN_AGREEMENT(
	id bigint comment '主键',
	artisan_id string comment '手艺人ID',
	status bigint comment '1有效；0无效',
	ctime string comment '创建时间',
	utime string comment '更新时间'
	)
	comment '手艺人协议表，高颜值手艺人用'
	PARTITIONED BY (partkey string)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
	STORED AS TEXTFILE
	LOCATION '/hive/db/ods/S02_ARTISAN_AGREEMENT';
 * 
 * @author wangguanru
 *
 */

public class TestTableCreate {

	public static void main(String[] args) {
		Map<String, String> map = new HashMap<String, String>();
		map.put("INT", "bigint");
		map.put("VARCHAR", "string");
		map.put("DATETIME", "string");
		map.put("BIGINT", "bigint");
		map.put("BOOLEAN", "bigint");
		map.put("TIMESTAMP", "string");
		map.put("DOUBLE", "decimal");
		map.put("DATE", "string");
		map.put("DECIMAL", "decimal");
		map.put("TINYINT", "bigint");
		map.put("INT UNSIGNED", "bigint");
		map.put("CHAR", "string");
		map.put("TEXT", "string");
		
		try {
			CsvReader cr = new CsvReader("metadata.csv");
			
			
			StringBuffer sb = new StringBuffer("");
			
			boolean flag = true;
			String tableName = "";
			String tableCnName = "";
			
			cr.readRecord();
			
			while(cr.readRecord()) {
				String record = new String(cr.getRawRecord().getBytes("iso8859-1"), "gbk");
				
				String tabName = record.split(",")[7];
				String tabCnName = record.split(",")[4];
				String colName = record.split(",")[11];
				String typeName = record.split(",")[14];
				String comName = record.split(",")[10];
				
				if(!tableName.equals(tabName)) {
					
					if(!"".equals(tableName)) {
						int index = sb.lastIndexOf(",\r\n");
						sb.replace(index, sb.length(), "\r\n");
						
						sb.append(")").append("\r\n");
						sb.append("comment '").append(tableCnName).append("'").append("\r\n");
						sb.append("PARTITIONED BY (partkey string)").append("\r\n");
						sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'").append("\r\n");
						sb.append("STORED AS TEXTFILE").append("\r\n");
						sb.append("LOCATION '/hive/db/ods/S02_").append(tableName.toUpperCase()).append("';").append("\r\n\r\n\r\n");
					}
					
					flag = true;
					tableName = tabName;
					tableCnName = tabCnName;
				}
				
				if(flag) {
					flag = false;
					sb.append("use ods;").append("\r\n");
					sb.append("drop table if exists ods.S02_").append(tabName.toUpperCase()).append(";").append("\r\n");
					sb.append("CREATE EXTERNAL TABLE ods.S02_").append(tabName.toUpperCase()).append("(").append("\r\n");
				}
				
				sb.append(colName).append(" ").append(typeName).append(" ").append("comment '").append(comName).append("',").append("\r\n");
				
			}
			
			if(!"".equals(tableName)) {
				int index = sb.lastIndexOf(",\r\n");
				sb.replace(index, sb.length(), "\r\n");
				
				sb.append(")").append("\r\n");
				sb.append("comment '").append(tableCnName).append("'").append("\r\n");
				sb.append("PARTITIONED BY (partkey string)").append("\r\n");
				sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'").append("\r\n");
				sb.append("STORED AS TEXTFILE").append("\r\n");
				sb.append("LOCATION '/hive/db/ods/S02_").append(tableName.toUpperCase()).append("';");
			}
			
			
			System.out.println(sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}