package java_test;

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


public class TestTableMetaData {

	public static void main(String[] args) {
	    String jdbc_url = "jdbc:mysql://10.0.10.140:3306/hlj?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&remarks=true&useInformationSchema=true";
		String jdbc_user_name = "hlj_op_read";
		String jdbc_password = "GNAmJNjQUXlfK27R";
		
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
			String[] head1 = new String[]{"SRC_DB_TYP", "SRC_SYS_COD", "SRC_SYS_DESC", "SRC_TAB_NAM", "SRC_TAB_CNAM", "EXP_TYP", "EDW_SYS_COD", "EDW_TAB_NAM", "EDW_EXP_JOB_DESC", "COL_ID", "COLUMN_COMMENTS", "SRC_COL_NAME", "EDW_COL_NAME", "SRC_DATA_TYPE", "EDW_DATA_TYPE", "DATA_LEN", "IS_NULL", "IS_PK", "IS_U_IND"};
			CsvWriter wr1 =new CsvWriter("metadata.csv",',',Charset.forName("GBK"));
			wr1.writeRecord(head1);
			
			String[] head2 = new String[]{"SRC_DB_TYP", "SRC_SYS_COD", "SRC_SYS_DESC", "Tab_owner", "SRC_TAB_NAM", "SRC_TAB_CNAM", "EXP_TYP", "EDW_SYS_COD", "EDW_SDATA_DBNAME", "EDW_TAB_NAM", "INC_WHERE", "SOURCE_DBNAME", "TARGET_DBNAME", "FULL_DATA_DATE"};
			CsvWriter wr2 =new CsvWriter("tablelist.csv",',',Charset.forName("GBK"));
			wr2.writeRecord(head2);
			
			String[] head3 = new String[]{"edw_job_cod", "edw_job_nam", "parameter_type", "parameter_value"};
			CsvWriter wr3 =new CsvWriter("pushparam.csv",',',Charset.forName("GBK"));
			wr3.writeRecord(head3);
			
			String[] head4 = new String[]{"SYSTEM", "ETL_JOB", "ETL_DESC", "JOB_TYPE", "FREQUENCY", "STREAM", "DEPENDENCY", "SRC_SCRIPT", "IS_TIMETRIGGER", "START_TIME", "GROUP_SERVER", "Creator"};
			CsvWriter wr4 =new CsvWriter("addjob.csv",',',Charset.forName("GBK"));
			wr4.writeRecord(head4);

			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(jdbc_url, jdbc_user_name, jdbc_password);
			DatabaseMetaData dbmd = conn.getMetaData();
			
			//得到Excel工作簿对象
			XSSFWorkbook wb = new XSSFWorkbook("tableinfo.xlsx");
			//得到Excel工作表对象
			XSSFSheet sheet = wb.getSheetAt(0);
			
			int rowCount = sheet.getLastRowNum();  
			
			int j = 0;
			for(j = 0; j <= rowCount; j++) {
				//得到Excel工作表的行    
				XSSFRow row = sheet.getRow(j);  
				//得到Excel工作表指定行的单元格    
//				XSSFCell tableNameCell = row.getCell((short) 0);  
//				XSSFCell tableCnNameCell = row.getCell((short) 1);  
//				XSSFCell expTypeCell = row.getCell((short) 2);  
//				XSSFCell edwTableNameCell = row.getCell((short) 3); 
				
				String tableName = row.getCell((short) 0).getStringCellValue();  
				
				System.out.println(tableName);
				
				String tableCnName = row.getCell((short) 1).getStringCellValue();  
				String expType = row.getCell((short) 2).getStringCellValue();  
				String edwTableName = row.getCell((short) 3).getStringCellValue(); 
				String owner = row.getCell((short) 4).getStringCellValue();
				String incCondition = null;
				if(row.getLastCellNum() == 6 && null != row.getCell((short) 5)) {
					incCondition = row.getCell((short) 5).getStringCellValue(); 
				}
				
				
				int i = 1;
				
				ResultSet tableRs = dbmd.getTables(null, null, tableName, null);
				
				tableRs = dbmd.getTables(null, null, null, null);
				
				
				while(tableRs.next()) {
					String tmpTableName = tableRs.getString("TABLE_NAME");  //表名  
		            String tableRemarks = tableRs.getString("REMARKS");       //表备注  
		            
		            ResultSet rs = dbmd.getColumns(null, null, tmpTableName, null);
					
					while(rs.next()) {
						String columnName = rs.getString("COLUMN_NAME");  //列名  
		                int dataType = rs.getInt("DATA_TYPE");     //对应的java.sql.Types的SQL类型(列类型ID)
		                int dateSize = rs.getInt("COLUMN_SIZE");     //对应的java.sql.Types的SQL类型(列类型ID)
		                String dataTypeName = rs.getString("TYPE_NAME");  //java.sql.Types类型名称(列类型名称)
		                String columnRemarks = rs.getString("REMARKS");
		                
		                String[] contentArray = new String[19];
		    			
		    			contentArray[0] = "MYSQL";
		    			contentArray[1] = "hlj";
		    			contentArray[2] = "db";
		    			contentArray[3] = "tb_name";
		    			contentArray[4] = "tb_cn_name";
		    			contentArray[5] = "FUL";
		    			contentArray[6] = "S02";
		    			contentArray[7] = "EDW_TAB_NAM";
		    			contentArray[8] = "";
		    			contentArray[9] = "1";
		    			contentArray[10] = "COLUMN_COMMENTS";
		    			contentArray[11] = "SRC_COL_NAME";
		    			contentArray[12] = "EDW_COL_NAME";
		    			contentArray[13] = "SRC_DATA_TYPE";
		    			contentArray[14] = "EDW_DATA_TYPE";
		    			contentArray[15] = "";
		    			contentArray[16] = "";
		    			contentArray[17] = "";
		    			contentArray[18] = "";
		                
		                contentArray[3] = tableName;
		                contentArray[4] = ((tableRemarks == null || tableRemarks.trim().equals(""))?tableCnName:tableRemarks);
		                contentArray[5] = expType;
		                contentArray[7] = edwTableName;
		                contentArray[9] = String.valueOf(i);
		                contentArray[10] = columnRemarks;
		    			contentArray[11] = columnName;
		    			contentArray[12] = columnName;
		    			contentArray[13] = (dataTypeName+"("+dateSize+")");
		    			if ("tinyint".equalsIgnoreCase(dataTypeName)) {
		    				contentArray[13] = (dataTypeName+"("+1+")");
		    			}
		    			contentArray[14] = map.get(dataTypeName);
		    			
		    			if(dataTypeName.equals("TEXT")) {
		    				String[] contentArray2 = new String[4];
			    			
		    				contentArray2[0] = "S02";
		    				contentArray2[1] = tableName;
		    				contentArray2[2] = "map-column-hive";
		    				contentArray2[3] = (columnName + "=STRING");
			    			
			    			wr3.writeRecord(contentArray2);
		    			}
		    			
		    			i++;
		    			wr1.writeRecord(contentArray);
					}
					
//					wr1.writeRecord(new String[]{""});
//					wr1.writeRecord(new String[]{""});
//					wr1.writeRecord(new String[]{""});
//					wr1.writeRecord(new String[]{""});
//					wr1.writeRecord(new String[]{""});
					
					String[] contentArray1 = new String[14];
	    			contentArray1[0] = "MYSQL";
	    			contentArray1[1] = "hlj";
	    			contentArray1[2] = "db";
	    			contentArray1[3] = "hlj";
	    			contentArray1[4] = "tb_name";
	    			contentArray1[5] = "tb_cn_name";
	    			contentArray1[6] = "FUL";
	    			contentArray1[7] = "S02";
	    			contentArray1[8] = "ODS";
	    			contentArray1[9] = "tb_name";
	    			contentArray1[10] = "";
	    			contentArray1[11] = "s2.db.pro.helijia.com";
	    			contentArray1[12] = "hadoop";
	    			contentArray1[13] = "";
	                
	                contentArray1[4] = tableName;
	                contentArray1[5] = ((tableRemarks == null || tableRemarks.trim().equals(""))?tableCnName:tableRemarks);
	                contentArray1[6] = expType;
	                contentArray1[9] = edwTableName;
	                if(expType.equals("INC")) {
	                	if ((incCondition == null || incCondition.trim().equals("")) && !tableName.equals("artisan_comment_photo")) {
	                		throw new Exception("增量表没有增量条件");
	                	}
	                	contentArray1[10] = "DATE_FORMAT(" + incCondition + ",\\'%Y-%m-%d\\') = \\'${yesterdayiso}\\' or DATE_FORMAT(last_update_time,\\'%Y-%m-%d\\') = \\'${yesterdayiso}\\'";
	                }
	                wr2.writeRecord(contentArray1);
	                
	                
	                String[] contentArray4 = new String[12];
	                contentArray4[0] = "S02";
	                contentArray4[1] = tableName.toUpperCase();
	                contentArray4[2] = ((tableRemarks == null || tableRemarks.trim().equals(""))?tableCnName:tableRemarks);
	                contentArray4[3] = "D";
	                contentArray4[4] = "";
	                contentArray4[5] = "";
	                contentArray4[6] = "";
	                contentArray4[7] = "/ETL/script/sqoopjob/pl/import.pl";
	                contentArray4[8] = "1";
	                contentArray4[9] = "00:59";
	                contentArray4[10] = "3";
	                contentArray4[11] = "奶牛";
	                
	                wr4.writeRecord(contentArray4);
				}
			}
			
			wr1.close();
			wr2.close();
			wr3.close();
			wr4.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
