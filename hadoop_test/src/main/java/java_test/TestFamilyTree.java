package java_test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TestFamilyTree {

	public static void main(String[] args) {
		new TestFamilyTree().calc();
	}
	
	private static final String bi_date = "20160828";
	private static final String date = "20160829";
	private static final String month = "201608";
	
	private static final String jdbc_url = "jdbc:mysql://10.0.120.7:6006/hlj?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&remarks=true&useInformationSchema=true";
	private static final String jdbc_user_name = "hlj_family";
	private static final String jdbc_password = "GNAmJNjQUXlfK27R";
	
	// memberId, parentId
	private HashMap<Integer, Integer> familyTree;
	// memberId, artisanCd
	private Map<Integer, String> memberIdArtisanCdMap;
	private Map<Integer, String> memberIdArtisanNickMap;
	private Map<Integer, String> memberIdArtisanTypeMap;
	private Map<Integer, String> memberIdArtisanCityMap;
	private Map<Integer, String> memberIdFamilyNameMap;
	// artisanCd, memberId
	private Map<String, Integer> artisanCdMemberIdMap;
	private List<Integer> activeMemberList;
	private List<Integer> fixLevelMemberList;
	
	// memberId, subFamilyCount
	private Map<Integer, Integer> subFamilyCountMemberId = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> subActiveFamilyCountMemberId = new HashMap<Integer, Integer>();
	// artisanCd, subFamilyCount
	private Map<String, Integer> subFamilyCountArtisanCd = new HashMap<String, Integer>();
	private Map<String, Integer> subActiveFamilyCountArtisanCd = new HashMap<String, Integer>();
	
	public void calc() {
		System.out.println("begin!!!");
		this.printFamilyMembers(4);
//		this.printGoldFamily(4);
	}
	
	// 打印直系黄金家族
	private void printGoldFamily(int familyLevel) {
		this.initData();
		this.initFixLevelData(familyLevel);
		for(Entry<Integer, Integer> entry : familyTree.entrySet()) {
			int memberId = entry.getKey();
			int goldParent = this.calcGoldFather(memberId, familyTree);
			System.out.println(memberIdArtisanNickMap.get(memberId) + "\t" + (goldParent != -99 ? memberIdArtisanNickMap.get(goldParent) : ""));
		}
	}
	
	// 打印某级家族以及所有家族成员  黄金是4  白银是3
	private void printFamilyMembers(int familyLevel) {
		this.initData();
		this.initFixLevelData(familyLevel);
		for (int memberId : fixLevelMemberList) {
			List<Integer> subFamilyMemberList = new ArrayList<Integer>();
			calcSubFamilyMembers(memberId, familyTree, subFamilyMemberList);
			for (Integer subMemberId : subFamilyMemberList) {
				System.out.println(memberIdArtisanNickMap.get(memberId) + "\t" + memberIdArtisanCdMap.get(memberId) + "\t" + memberIdArtisanCdMap.get(subMemberId));
			}
		}
	}
	
	// 打印家族信息
	private void printSubFamily() {
		this.initData();
		this.initActiveMemberIdListFormal();
		try {
			System.out.println("家族ID\t手艺人编号\t家族名称\t类目\t城市\t子家族数量\t子活跃家族");
			
			for(Entry<Integer, Integer> entry : familyTree.entrySet()) {
				int memberId = entry.getKey();
				int subTreeCount = this.calcSubFamily(memberId, 1, familyTree);
				subFamilyCountMemberId.put(memberId, subTreeCount);
				subFamilyCountArtisanCd.put(memberIdArtisanCdMap.get(memberId), subTreeCount);
				
				int subActiveTreeCount = 0;
				if (activeMemberList.contains(memberId)) {
					subActiveTreeCount = this.calcSubActiveFamily(memberId, 1, activeMemberList, familyTree);
				} else {
					subActiveTreeCount = this.calcSubActiveFamily(memberId, 0, activeMemberList, familyTree);
				}
				subActiveFamilyCountMemberId.put(memberId, subActiveTreeCount);
				subActiveFamilyCountArtisanCd.put(memberIdArtisanCdMap.get(memberId), subActiveTreeCount);
			}
			
			StringBuilder sb = new StringBuilder("");
			for(Entry<Integer, String> entry : memberIdArtisanCdMap.entrySet()) {
				sb.append(",'" + entry.getValue() + "'");
			}
			
			for(Entry<Integer, Integer> entry : familyTree.entrySet()) {
				int memberId = entry.getKey();
				
				if (subFamilyCountMemberId.containsKey(memberId)) {
					System.out.println(memberId 
							+ "\t" + memberIdArtisanCdMap.get(memberId) 
							+ "\t" + memberIdFamilyNameMap.get(memberId) 
							+ "\t" + memberIdArtisanTypeMap.get(memberId) 
							+ "\t" + memberIdArtisanCityMap.get(memberId) 
							+ "\t" + subFamilyCountMemberId.get(memberId) 
							+ "\t" + subActiveFamilyCountMemberId.get(memberId));
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initData() {
		familyTree = new HashMap<Integer, Integer>();
		
		memberIdArtisanCdMap = new HashMap<Integer, String>();
		memberIdArtisanNickMap = new HashMap<Integer, String>();
		memberIdArtisanTypeMap = new HashMap<Integer, String>();
		memberIdArtisanCityMap = new HashMap<Integer, String>();
		memberIdFamilyNameMap = new HashMap<Integer, String>();
		
		artisanCdMemberIdMap = new HashMap<String, Integer>();
		
		String jdbc_url = "jdbc:mysql://10.0.120.7:6006/hlj?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&remarks=true&useInformationSchema=true";
		String jdbc_user_name = "hlj_op_read";
		String jdbc_password = "GNAmJNjQUXlfK27R";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(jdbc_url, jdbc_user_name, jdbc_password);
			
			// 获取成立家族
			String mainSql = "select t1.member_id, t1.parent_id, t2.artisan_code, t2.nick, t3.city_name, t4.name type_name, t5.name family_name "
					+ "from member_daily_info t1 "
					+ "join artisan t2 on t1.user_id = t2.artisan_id "
					+ "join hlj_report.open_city t3 on t2.city = t3.id "
					+ "join artisan_type t4 on t2.type = t4.type "
					+ "join member t5 on t1.member_id = t5.id "
					+ "where t1.parents = 1 and t1.status = 1 and date_format(t1.data_date, '%Y%m%d') = '" + date + "' "
					+ "and t1.user_id not in (select artisan_id from hlj_report.tmp_artisan_test)";
			
			// 获取所有的
			mainSql = "select t1.member_id, t1.parent_id, t2.artisan_code, t2.nick, t3.city_name, t4.name type_name, t5.name family_name "
					+ "from member_daily_info t1 "
					+ "join artisan t2 on t1.user_id = t2.artisan_id "
					+ "join hlj_report.open_city t3 on t2.city = t3.id "
					+ "join artisan_type t4 on t2.type = t4.type "
					+ "join member t5 on t1.member_id = t5.id "
					+ "where t1.status = 1 and date_format(t1.data_date, '%Y%m%d') = '" + date + "' "
					+ "and t1.user_id not in (select artisan_id from hlj_report.tmp_artisan_test)";
			
			Statement mainStat = conn.createStatement();
			ResultSet mainRs = mainStat.executeQuery(mainSql);
			while(mainRs.next()) {
				int memberId = mainRs.getInt("member_id");  //列名
				int parentId = mainRs.getInt("parent_id");  //列名
				String artisanCode = mainRs.getString("artisan_code");  //列名
				String nick = mainRs.getString("nick");  //列名
				String cityName = mainRs.getString("city_name");  //列名
				String typeName = mainRs.getString("type_name");  //列名
				String familyName = mainRs.getString("family_name");  //列名
				
				familyTree.put(memberId, parentId);
				memberIdArtisanCdMap.put(memberId, artisanCode);
				memberIdArtisanNickMap.put(memberId, nick);
				memberIdArtisanTypeMap.put(memberId, cityName);
				memberIdArtisanCityMap.put(memberId, typeName);
				memberIdFamilyNameMap.put(memberId, familyName);
				
				artisanCdMemberIdMap.put(artisanCode, memberId);
			}
			
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initFixLevelData(int familyLevel) {
		fixLevelMemberList = new ArrayList<Integer>();
		
		String jdbc_url = "jdbc:mysql://10.0.120.7:6006/hlj?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&remarks=true&useInformationSchema=true";
		String jdbc_user_name = "hlj_op_read";
		String jdbc_password = "GNAmJNjQUXlfK27R";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(jdbc_url, jdbc_user_name, jdbc_password);
			
			String mainSql = "select t1.member_id from member_daily_info t1 "
					+ "where t1.family_level = " + familyLevel + " and t1.status = 1 and date_format(t1.data_date, '%Y%m%d') = '" + date + "' "
					+ "and t1.user_id not in (select artisan_id from hlj_report.tmp_artisan_test) and t1.user_id in (select artisan_id from artisan where city = 2)";
			
			Statement mainStat = conn.createStatement();
			ResultSet mainRs = mainStat.executeQuery(mainSql);
			while(mainRs.next()) {
				int memberId = mainRs.getInt("member_id");  //列名
				
				fixLevelMemberList.add(memberId);
			}
			
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private List<Integer> initActiveMemberIdList() {
		activeMemberList = new ArrayList<Integer>();
		
		try {
			String activeSql = "select distinct t1.artisan_cd from rpt_family_m t1 "
					+ "join tmp_family_target_price t2 on t1.artisan_cd = t2.artisan_cd "
					+ "where t1.family_active_price >= t2.target_price and date_format(t1.calc_dt, '%Y%m%d') = '" + bi_date + "'";
			
			String jdbc_url = "jdbc:mysql://10.0.10.12:3306/app?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&remarks=true&useInformationSchema=true";
			String jdbc_user_name = "dwetl";
			String jdbc_password = "bietlmetadb";
			
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(jdbc_url, jdbc_user_name, jdbc_password);
			
			Statement activeStat = conn.createStatement();
			ResultSet activeRs = activeStat.executeQuery(activeSql);
			while(activeRs.next()) {
				String artisanCode = activeRs.getString("artisan_cd");  //列名
				activeMemberList.add(artisanCdMemberIdMap.get(artisanCode));
			}
			
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return activeMemberList;
	}
	
	private List<Integer> initActiveMemberIdListFormal() {
		activeMemberList = new ArrayList<Integer>();
		
		try {
			String activeSql = "select distinct member_id from family_active_status_record where level_month = '" + month + "' and family_active_status = 1";
			
			String jdbc_url = "jdbc:mysql://10.0.120.7:6006/hlj?useUnicode=true&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&remarks=true&useInformationSchema=true";
			String jdbc_user_name = "hlj_op_read";
			String jdbc_password = "GNAmJNjQUXlfK27R";
			
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(jdbc_url, jdbc_user_name, jdbc_password);
			
			Statement activeStat = conn.createStatement();
			ResultSet activeRs = activeStat.executeQuery(activeSql);
			while(activeRs.next()) {
				int memberId = activeRs.getInt("member_id");  //列名
				activeMemberList.add(memberId);
			}
			
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return activeMemberList;
	}
	
	private int calcSubFamily(int memberId, int count, HashMap<Integer, Integer> familyTree) {
		for(Entry<Integer, Integer> entry : familyTree.entrySet()) {
			if (memberId == entry.getValue()) {
				count += this.calcSubFamily(entry.getKey(), 1, familyTree);
			}
		}
		
		return count;
	}
	
	private int calcSubActiveFamily(int memberId, int count, List<Integer> activeMemberList, HashMap<Integer, Integer> familyTree) {
		for(Entry<Integer, Integer> entry : familyTree.entrySet()) {
			if (memberId == entry.getValue()) {
				if (activeMemberList.contains(entry.getKey())) {
					count += this.calcSubActiveFamily(entry.getKey(), 1, activeMemberList, familyTree);
				} else {
					count += this.calcSubActiveFamily(entry.getKey(), 0, activeMemberList, familyTree);
				}
			}
		}
		
		return count;
	}
	
	private void calcSubFamilyMembers(int memberId, HashMap<Integer, Integer> familyTree, List<Integer> familyMemberList) {
		for(Entry<Integer, Integer> entry : familyTree.entrySet()) {
			if (memberId == entry.getValue()) {
				familyMemberList.add(entry.getKey());
				calcSubFamilyMembers(entry.getKey(), familyTree, familyMemberList);
			}
		}
	}
	
	private int calcGoldFather(int memberId, HashMap<Integer, Integer> familyTree) {
		if (fixLevelMemberList.contains(memberId)) {
			return memberId;
		} else if (familyTree.get(memberId) == null) {
			return -99;
		} else {
			return calcGoldFather(familyTree.get(memberId), familyTree);
		}
	}
	
}
