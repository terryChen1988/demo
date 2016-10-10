package java_test;

import java.io.FileWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.csvreader.CsvWriter;


public class TestTableMetaDataHive {

	public static void main(String[] args) {
		
		String driverName = "org.apache.hive.jdbc.HiveDriver";
	    String url = "jdbc:hive2://10.0.50.227:10000/default";
	    String user = "phive";
	    String password = "phive";
		
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
			
			Class.forName(driverName);
			Connection conn = DriverManager.getConnection(url, user, password);
			DatabaseMetaData dbmd = conn.getMetaData();
			
			//得到Excel工作簿对象
			XSSFWorkbook wb = new XSSFWorkbook("/Users/wangguanru/Downloads/hive-spark.xlsx");
			//得到Excel工作表对象
			XSSFSheet sheet = wb.getSheetAt(0);
			
			int rowCount = sheet.getLastRowNum();  
			
			int j = 0;
			for(j = 1; j <= rowCount; j++) {
				//得到Excel工作表的行    
				XSSFRow row = sheet.getRow(j);  
				
				String sysName = row.getCell((short) 3).getStringCellValue();  
				String tabName = row.getCell((short) 1).getStringCellValue();  
				
				ResultSet tableRs = dbmd.getTables(null, null, tabName, null);
				
				while(tableRs.next()) {
					String tmpTableName = tableRs.getString("TABLE_NAME");  //表名
		            String tableRemarks = tableRs.getString("REMARKS");       //表备注
		            
		            sb.append("use " + sysName + ";").append("\n");
					sb.append("drop table if exists " + sysName + "." + tmpTableName + "_spark;\n");
					sb.append("CREATE EXTERNAL TABLE " + sysName + "." + tmpTableName + "_spark(\n");

					ResultSet rs = dbmd.getColumns(null, null, tmpTableName, null);
					
					while(rs.next()) {
						String columnName = rs.getString("COLUMN_NAME");  //列名
		                int dataType = rs.getInt("DATA_TYPE");     //对应的java.sql.Types的SQL类型(列类型ID)
		                int dataSize = rs.getInt("COLUMN_SIZE");     //对应的java.sql.Types的SQL类型(列类型ID)
		                int decimalDigits = rs.getInt("DECIMAL_DIGITS");
		                String dataTypeName = rs.getString("TYPE_NAME");  //java.sql.Types类型名称(列类型名称)
		                String columnRemarks = (rs.getString("REMARKS") == null? "":rs.getString("REMARKS"));
		                
		                if ("partkey".equals(columnName) || "hour".equals(columnName)) {
		                	continue;
		                }
		                
		                if ("DECIMAL".equals(dataTypeName)) {
		                	sb.append(columnName).append(" ").append(dataTypeName).append("(").append(dataSize).append(", ").append(decimalDigits).append(") ").append("comment '").append(columnRemarks).append("',").append("\n");
		                } else {
		                	sb.append(columnName).append(" ").append(dataTypeName).append(" ").append("comment '").append(columnRemarks).append("',").append("\n");
		                }
					}
					
					int index = sb.lastIndexOf(",\n");
					sb.replace(index, sb.length(), "\n");
					
					sb.append(")").append("\n");
					sb.append("comment '").append(tableRemarks).append("'").append("\n");
					if ((tmpTableName.toLowerCase().endsWith("_hh") || tmpTableName.toLowerCase().indexOf("_hh_") >= 0) 
							&& !sysName.toLowerCase().equals("dw") 
							&& !sysName.toLowerCase().equals("dws")) {
						sb.append("PARTITIONED BY (partkey string, hour string)").append("\n");
					} else {
						sb.append("PARTITIONED BY (partkey string)").append("\n");
					}
					sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001'").append("\n");
					sb.append("STORED AS PARQUET").append("\n");
					sb.append("LOCATION '/hive/db/" + sysName + "/" + (tmpTableName + "_spark").toUpperCase()).append("';").append("\n\n\n");
				}
				
				System.out.println(sb.toString());
				
				FileWriter fw = new FileWriter("hive-spark-create-table.hql");
				fw.write(sb.toString());
				fw.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
