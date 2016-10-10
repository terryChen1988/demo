package java_test;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

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

public class TestTableCreateExcel {

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
			StringBuffer sb = new StringBuffer("");
			
			boolean flag = true;
			String tableName = "";
			String tableCnName = "";
			
			//得到Excel工作簿对象    
			XSSFWorkbook wb = new XSSFWorkbook("/Users/wangguanru/Documents/bi/sddl范例 (自动保存的).xlsx");  
			//得到Excel工作表对象    
			XSSFSheet sheet = wb.getSheetAt(2);   
			
			int rowCount = sheet.getLastRowNum();  
			
			int j = 0;
			for(j = 1; j < rowCount; j++) {
				//得到Excel工作表的行    
				XSSFRow row = sheet.getRow(j);  
				//得到Excel工作表指定行的单元格    
//				XSSFCell tableNameCell = row.getCell((short) 0);  
//				XSSFCell tableCnNameCell = row.getCell((short) 1);  
//				XSSFCell expTypeCell = row.getCell((short) 2);  
//				XSSFCell edwTableNameCell = row.getCell((short) 3);
				
				String tabName = row.getCell((short) 7).getStringCellValue();
				String tabCnName = row.getCell((short) 4).getStringCellValue();
				String colName = row.getCell((short) 11).getStringCellValue();
				String typeName = row.getCell((short) 14).getStringCellValue();
				String comName = (row.getCell((short) 10) == null?"":row.getCell((short) 10).getStringCellValue().replaceAll(";", "；"));
				
				if(!tableName.equals(tabName)) {
					
					if(!"".equals(tableName)) {
						int index = sb.lastIndexOf(",\n");
						sb.replace(index, sb.length(), "\n");
						
						sb.append(")").append("\n");
						sb.append("comment '").append(tableCnName).append("'").append("\n");
						sb.append("PARTITIONED BY (partkey string)").append("\n");
						sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'").append("\n");
						sb.append("STORED AS TEXTFILE").append("\n");
						sb.append("LOCATION '/hive/db/ods/S02_").append(tableName.toUpperCase()).append("';").append("\n\n\n");
					}
					
					flag = true;
					tableName = tabName;
					tableCnName = tabCnName;
				}
				
				if(flag) {
					flag = false;
					sb.append("use ods;").append("\n");
					sb.append("drop table if exists ods.S02_").append(tabName.toUpperCase()).append(";").append("\n");
					sb.append("CREATE EXTERNAL TABLE ods.S02_").append(tabName.toUpperCase()).append("(").append("\n");
				}
				
				sb.append(colName).append(" ").append(typeName).append(" ").append("comment '").append(comName).append("',").append("\n");
				
			}
			
			if(!"".equals(tableName)) {
				int index = sb.lastIndexOf(",\n");
				sb.replace(index, sb.length(), "\n");
				
				sb.append(")").append("\n");
				sb.append("comment '").append(tableCnName).append("'").append("\n");
				sb.append("PARTITIONED BY (partkey string)").append("\n");
				sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'").append("\n");
				sb.append("STORED AS TEXTFILE").append("\n");
				sb.append("LOCATION '/hive/db/ods/S02_").append(tableName.toUpperCase()).append("';");
			}
			
			System.out.println(sb.toString());
			
//			File file = new File("createTable.hql");
			
			FileWriter fw = new FileWriter("createTable.hql");
			fw.write(sb.toString());
			fw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}