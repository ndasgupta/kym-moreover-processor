package main;

import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import dbconnect.general.moreover_sources_statements;
import dbconnect.general.product_statements;
import dbconnect.main.DBConnect;

//TODO: not currently set up to read all info. Check notes to complete this.

public class Main {

	public static final int MILLIS_PER_MINUTE = 60000;
	public static final int FIXED_RUN_TIME_MINUTES = 12;
	public static final int FIXED_RUN_TIME_MILLIS = FIXED_RUN_TIME_MINUTES*MILLIS_PER_MINUTE;
	public static final String mainThreadStamp = "(reader main) ";
	public static final List<String> categoryList = new  ArrayList<>(Arrays.asList(
			"academic",
			"general",
			"government",
			"journal",
			"local",
			"national",
			"organization"));
	public static final List<String> editorialRankList = new ArrayList<>(Arrays.asList(
			"1","2","3","4"));
	
	
	public static final String GEN_LIST_QUERY = "select distinct d.name, d.combination_id, d.id "
			+ "from product b, combination c, generic_name d "
			+ "where b.type != 'HUMAN OTC DRUG' "
			+ "and b.combination_id = c.id "
			+ "and d.combination_id = c.id";
	
	public static final int CACHECOUNT = 20;

	
	//TODO: write articles in new threads. 
	
	public static void main(String[] args) {
		
		//initialize maps so all runnables and threads can be accurately referenced
		HashMap<Integer,CacheReaderRunnable> runnableMap = 
				new HashMap<Integer,CacheReaderRunnable>();
		HashMap<Integer,Thread> threadMap = new HashMap<Integer,Thread>();
		
		long startTime = System.currentTimeMillis();
		
		try {
			
			HashMap<String,Integer> combinationIdMap = new HashMap<String,Integer>();
			HashMap<String,Integer> genericNameMap = readGenList(combinationIdMap);
			HashMap<String,Integer> productMap = readProdList(genericNameMap,combinationIdMap);
			
			

			System.out.println(mainThreadStamp + "combination ids found: " + combinationIdMap.size());
						
			//initialize cache reader threads, and index them in the maps
			for (int i = 0; i < CACHECOUNT; i++) {
				CacheReaderRunnable reader = new CacheReaderRunnable();
				reader.cacheNum = i;
				reader.genericNameMap = genericNameMap;
				reader.productMap = productMap;
				reader.combinationIdMap = combinationIdMap;
				reader.categoryList = categoryList;
				reader.editorialRankList = editorialRankList;
				Thread t = new Thread(reader);
				
				t.start();
				
				runnableMap.put(i, reader);
				threadMap.put(i, t);
			}
			
			//TODO: check if any thread has been terminated (exception outside loop). restart it if so.
			while (true) {
				int exceptionCount = 0;
				for (CacheReaderRunnable r: runnableMap.values()) {
					exceptionCount+=r.exceptionCount;
				}
				Thread.sleep(60000);
				printCurrentRunTime(startTime, "current run time (" + exceptionCount 
						+ ") exceptions");
			}
			
		} catch (Exception e) {
			System.out.println(mainThreadStamp + "CONTENTPROCESSOR MAIN THREAD EXCEPTION");
			e.printStackTrace();
		}
		
	}
	/*================================================================================
	 * checkTimeLimit
	 *===============================================================================*/
	protected static boolean checkTimeLimit(long startTime) throws Exception{		
		if (getCurrentRunTime(startTime) >= FIXED_RUN_TIME_MILLIS) {
			System.out.println(mainThreadStamp + "time limit reached (" 
					+ FIXED_RUN_TIME_MINUTES + " min). terminating threads.");
			return true;
		}
		
		return false;
	}
	
	/*================================================================================
	 * readGenList: compiles list of unique drugs
	 *===============================================================================*/
	protected static HashMap<String,Integer> readGenList(HashMap<String,Integer> combIdMap) 
			throws Exception{

		long startTime = System.currentTimeMillis();
		
		HashMap<String,Integer> genMap = new HashMap<String,Integer>();
		
		DBConnect con = new DBConnect();
		connectToDatabase(con);
		ResultSet rsGen = con.ExecuteQuery(GEN_LIST_QUERY);
		con.CommitClose();	
		
		//constuct string list from result set
		while (rsGen.next()) {
			String temp_drugname =  rsGen.getString("name");
			if (temp_drugname == null) {
				throw new Exception("null drugname found");
			} else {
				temp_drugname = temp_drugname.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
			}
			genMap.put(temp_drugname, rsGen.getInt("id"));
			combIdMap.put(temp_drugname, rsGen.getInt("combination_id"));
		}
		
		System.out.println(mainThreadStamp + "read " + genMap.size() + " generic names");			

		System.out.println(mainThreadStamp + "run time:  " + 
				((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return genMap;
	}
	/*================================================================================
	 * readProdList: compiles list of unique drugs
	 *===============================================================================*/
	protected static HashMap<String,Integer> readProdList(HashMap<String,Integer> genMap, 
			HashMap<String,Integer> combIdMap) throws Exception{

		long startTime = System.currentTimeMillis();
		HashMap<String,Integer> prodMap = new HashMap<String,Integer>();

		//intialize statement
		String selectQueryProd = product_statements.Select(new ArrayList<>(
				Arrays.asList("name","id","combination_id","type")));
		
		//connect and execute statement/retreive data
		DBConnect con = new DBConnect();
		connectToDatabase(con);
		ResultSet rsProd = con.ExecuteQuery(selectQueryProd);
		con.CommitClose();	
		
		//iterate through result list and put entries into product map.
		while (rsProd.next()) {
			String temp_drugname =  rsProd.getString("name");
			
			//apply all rules, so only the desired names are stored
			if (temp_drugname == null) {
				throw new Exception("null drugname found");
			} 
			
			//filter by type
			if (rsProd.getString("type").trim().equalsIgnoreCase("HUMAN OTC DRUG")) {
				continue;
			}
			
			temp_drugname = temp_drugname.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
			
			//store in map.
			prodMap.put(temp_drugname, rsProd.getInt("id")); 
			combIdMap.put(temp_drugname, rsProd.getInt("combination_id"));	
		}
		
		System.out.println(mainThreadStamp + "read " + prodMap.size() + " product names");			

		System.out.println(mainThreadStamp + "run time:  " + 
				((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return prodMap;
	}
	/*================================================================================
	 * readSources: compiles list of relevant sources from moreover_sources, name and
	 * sourceid columns
	 *===============================================================================*/
	protected static HashMap<Long, String> readSources() throws Exception{

		long startTime = System.currentTimeMillis();
		
		HashMap<Long, String> sourceMap = new HashMap<Long, String>();
		
		//construct and execute select statement for druglist
		String selectQuery = moreover_sources_statements.Select(new ArrayList<>(
				Arrays.asList("sourceid","name")));
		DBConnect con = new DBConnect();
		connectToDatabase(con);
		ResultSet rs = con.ExecuteQuery(selectQuery);
		con.CommitClose();	
		
		//constuct string list from result set
		while (rs.next()) {
			String temp_sourceName =  rs.getString("name");
			Long temp_sourceId = rs.getLong("sourceid");
			if (temp_sourceName == null) {
				throw new Exception("null source name found");
			} else if (temp_sourceId == 0) {
				throw new Exception("null source id found");
			}
			sourceMap.put(temp_sourceId, temp_sourceName);
		}
		
		System.out.println(mainThreadStamp + "read " + sourceMap.size() + 
				" sources from moreover_sources table");			

		System.out.println(mainThreadStamp + "run time:  " + 
				((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return sourceMap;
	}
	/*================================================================================
	 * removeDuplicates
	 *===============================================================================*/
	public static List<String> removeDuplicates(List<String> targetList) {
		
		for (String s: targetList) {
			s = s.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
		}
		
		Collections.sort(targetList);
		int i = 0;
		while (i+1 < targetList.size()) {			
			if (targetList.get(i).equals(targetList.get(i+1))) {
				targetList.remove(i);
				continue;
			}			
			i++;
		}
		return targetList;

	}
	/*================================================================================
	 * getCurrentRunTime
	 *===============================================================================*/
	protected static long getCurrentRunTime(long startTime) {
		return System.currentTimeMillis() - startTime;
	}
	/*================================================================================
	 * printCurrentRunTime: given start time, prints current run time in seconds
	 *===============================================================================*/
	protected static long printCurrentRunTime(long startTime, String text) {
		DecimalFormat df = new DecimalFormat("#.00");
		long runTime = (System.currentTimeMillis() - startTime)/1000;
		String runTimeString = df.format(runTime);
		System.out.println(mainThreadStamp + text + ": " + runTimeString + " sec");
		return runTime;
	}
	/*================================================================================
	 * databaseConnection
	 *===============================================================================*/
	protected static void connectToDatabase(DBConnect connect) throws Exception {
		connect.ConnectProd();
	}
	
}
