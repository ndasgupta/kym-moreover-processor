package main;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import dbconnect.general.condition_statements;
import dbconnect.general.document_relation_row;
import dbconnect.general.moreover_query_statements;
import dbconnect.general.product_statements;
import dbconnect.main.DBConnect;

public class Filter {
	
	public int keyWordMatchThreshold = 3;
	private List<String> keyWordList;
	private HashMap<String, Integer> conditionsMap;
	private HashMap<String,Integer> combinationIdMap;
	private HashMap<String,Integer> genericNameMap;
	private HashMap<String,Integer> productMap;
	private HashMap<String, Integer> queryMap;
	public final List<String> categoryList = new  ArrayList<>(Arrays.asList(
			"academic",
			"general",
			"government",
			"journal",
			"local",
			"national",
			"organization"));
	public final List<String> editorialRankList = new ArrayList<>(Arrays.asList(
			"1","2","3","4"));
	public static final String GEN_LIST_QUERY = "select distinct d.name, d.combination_id, d.id "
			+ "from product b, combination c, generic_name d "
			+ "where b.type != 'HUMAN OTC DRUG' "
			+ "and b.combination_id = c.id "
			+ "and d.combination_id = c.id";
	
	public static final int SNIPPET_LENGTH = 1000;
	public static final int MIN_NAME_LENGTH = 3;
	
	public boolean initialized = true;
	
	/*================================================================================
	 * init: initialize all of the filter values.
	 *===============================================================================*/
	public void init(String threadStamp, int matchThreshold) throws Exception {

		keyWordMatchThreshold = matchThreshold;

		//populate information from database
		keyWordList = readKeyWords(threadStamp);
		queryMap = readQueryList(threadStamp);

		combinationIdMap = new HashMap<String,Integer>();
		genericNameMap = new HashMap<String,Integer>();
		productMap = new HashMap<String,Integer>();
		printToConsole(threadStamp, "combination ids found: " + combinationIdMap.size());
		conditionsMap = readConditionList(threadStamp);
//		combinationIdMap = new HashMap<String,Integer>();
//		genericNameMap = readGenList(combinationIdMap);
//		productMap = readProdList(genericNameMap,combinationIdMap);
//		printToConsole("combination ids found: " + combinationIdMap.size());
		
		initialized = true;
	}
	/*================================================================================
	 * checkConditions: checks the title and first 1000 characters of the article for
	 * conditions.
	 *===============================================================================*/
	public void checkConditions(MoreoverArticle article) throws Exception {
		
		if (!initialized) { throw new Exception("filter not initialized"); }
		
		Set<String> conditionsFound = new HashSet<String>();
		for (String c: conditionsMap.keySet()) {
			if (doesMatch(article.title, c) || doesMatch(article.content, c)) {
				conditionsFound.add(c);
			}
		}
		article.conditionsFound = conditionsFound;
	}
	/*================================================================================
	 * checkDrugNames: checks the title and first 1000 characters of the article for
	 * conditions.
	 *===============================================================================*/
	public void checkDrugNames(MoreoverArticle article) throws Exception {
		
		if (!initialized) { throw new Exception("filter not initialized"); }
		
		Set<String> namesFound = new HashSet<String>();
		String relevanceValue = null;
		
		for (String q: queryMap.keySet()) {
			if (doesMatch(article.title, q)) {
				namesFound.add(q);
				relevanceValue = MoreoverArticle.RELEVANCY_TITLE_VAL;
			} else if (doesMatch(article.content, q)) {
				namesFound.add(q);
				if (relevanceValue == null) { relevanceValue = MoreoverArticle.RELEVANCY_CONTENT_VAL; }
			}
		}
		
		for (String g: genericNameMap.keySet()) {
			if (doesMatch(article.title, g)) {
				namesFound.add(g);
				relevanceValue = MoreoverArticle.RELEVANCY_TITLE_VAL;
			} else if (doesMatch(article.content, g)) {
				namesFound.add(g);
				if (relevanceValue == null) { relevanceValue = MoreoverArticle.RELEVANCY_CONTENT_VAL; }
			}
		}
		for (String p: productMap.keySet()) {
			if (doesMatch(article.title, p)) {
				namesFound.add(p);
				relevanceValue = MoreoverArticle.RELEVANCY_TITLE_VAL;
			} else if (doesMatch(article.content, p)) {
				namesFound.add(p);
				if (relevanceValue == null) { relevanceValue = MoreoverArticle.RELEVANCY_CONTENT_VAL; }
			}
		}		
		article.drugsFound = namesFound;
		article.relevanceValue = relevanceValue;
	}
	/*================================================================================
	 * checkRelevantArticle: checks whether or not article is desired/relevant. If
	 * relevant, update the article to represent relevance values, and return true.
	 *===============================================================================*/
	protected boolean checkRelevantArticle(MoreoverArticle article) throws Exception {		
		
		if (!initialized) { throw new Exception("filter not initialized"); }
		
		//FILTER 1: check that the article source is relevant.
		boolean categoryMatch = false;
		for (String c: categoryList) {
			if (c.trim().toLowerCase().equals(article.category.trim().toLowerCase())) {
				categoryMatch = true;
				break;
			}
		}
		boolean rankMatch = false;
		for (String r: editorialRankList) {
			if (r.trim().toLowerCase().equals(article.editorialRank.trim().toLowerCase())) {
				rankMatch = true;
				break;
			}
		}
		if (!categoryMatch || !rankMatch) {
			return false;
		}
		
		//FILTER 2: horizontal keyword search. only move forward if the requisite amount of
		//keywords have appeared in the article.
		int keyWordMatchCount = 0;
		for (String keyWord: keyWordList) {
			if (keyWordMatchCount >= keyWordMatchThreshold) { break; }
			if (doesMatch(article.title, keyWord) || doesMatch(article.content, keyWord)){
				keyWordMatchCount++;
				continue;
			}
		}
		if ((keyWordMatchCount < keyWordMatchThreshold) && keyWordList.size() > 0) {
			return false;
		}
		

		//FILTER 3: do a cursory check to determine whether or not the article has drug information
		boolean isRelevant = false;
		String firstDrugFound = null;
		for (String drug: queryMap.keySet()) {
			if (doesMatch(article.title, drug) || doesMatch(article.content, drug)) {
				firstDrugFound = drug;
				isRelevant = true;
				break;
			}
		}

		for (String drug: genericNameMap.keySet()) {
			if (doesMatch(article.title, drug) || doesMatch(article.content, drug)){
				firstDrugFound = drug;
				isRelevant = true;
				break;
			}
		}	
		if (!isRelevant) {
			for (String drug: productMap.keySet()) {
				if (doesMatch(article.title, drug) || doesMatch(article.content, drug)){
					firstDrugFound = drug;
					isRelevant = true;
					break;
				}
			}
		}
		
		if (isRelevant) {
			article.declareRelevant(firstDrugFound);
		}
		return isRelevant;		
	}
	
	/*================================================================================
	 * doesMatch: checks a body of text 'content' for appearance of a string 'query'
	 *===============================================================================*/
	protected static boolean doesMatch(String content, String query) {
		
		//return if the query is too short.
		if( query.replaceAll("[^a-zA-Z0-9]","").trim().length() <= MIN_NAME_LENGTH){
			return false;
		}		
		content = content.length() >= SNIPPET_LENGTH ? content.substring(0, 
				SNIPPET_LENGTH-1) : content;
		content = content.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
		
		query = query.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
		if (content.contains(query)) {
			return true ;
		}
		return false ;
	}
	/*================================================================================
	 * initRelationsRows: intializes a database relation row
	 *===============================================================================*/
	public List<document_relation_row> initRelationsRows(MoreoverArticle article) 
	throws Exception {
		
		if (!initialized) { throw new Exception("filter not initialized"); }
		
		List<document_relation_row> relations = new Vector<document_relation_row>();
		for (String s: article.drugsFound) {
			document_relation_row docRel = new document_relation_row();
			docRel.query_id = queryMap.get(s);
			docRel.generic_name_id = genericNameMap.get(s);
			docRel.product_id = productMap.get(s);
			docRel.combination_id = combinationIdMap.get(s);
			relations.add(docRel);
		}
		for (String s: article.conditionsFound) {
			document_relation_row docRel = new document_relation_row();
			docRel.condition_id = conditionsMap.get(s);
			relations.add(docRel);
		}
		return relations;
	}
	
	/*================================================================================
	 * readConditionList: compiles list of unique drugs
	 *===============================================================================*/
	protected static HashMap<String,Integer> readConditionList(String threadStamp) throws Exception{

		long startTime = System.currentTimeMillis();
		
		HashMap<String,Integer> condMap = new HashMap<String,Integer>();
		
		//intialize statement
		String selectQueryProd = condition_statements.Select(new ArrayList<>(
						Arrays.asList("id","name")));
		
		DBConnect con = new DBConnect();
		Main.connectToDatabase(con);
		ResultSet rsCond = con.ExecuteQuery(selectQueryProd);
		con.CommitClose();	
		
		//constuct string list from result set
		while (rsCond.next()) {
			String temp_condition =  rsCond.getString("name");
			if (temp_condition == null) {
				throw new Exception("null drugname found");
			} else {
				temp_condition = temp_condition.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
			}
			condMap.put(temp_condition, rsCond.getInt("id"));
		}
		
		printToConsole(threadStamp, "read " + condMap.size() + " conditions");			
		printToConsole(threadStamp, "run time:  " + 
				((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return condMap;
	}
	/*================================================================================
	 * readGenList: compiles list of unique drugs
	 *===============================================================================*/
	protected static HashMap<String,Integer> readGenList(String threadStamp, 
	HashMap<String,Integer> combIdMap) throws Exception{

		long startTime = System.currentTimeMillis();
		
		HashMap<String,Integer> genMap = new HashMap<String,Integer>();
		
		DBConnect con = new DBConnect();
		Main.connectToDatabase(con);
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
		
		printToConsole(threadStamp, "read " + genMap.size() + " generic names");			
		printToConsole(threadStamp, "run time:  " + ((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return genMap;
	
	}
	/*================================================================================
	 * readKeyWords: reads list of keywords from an external file
	 *===============================================================================*/
	protected static List<String> readKeyWords(String threadStamp) {
		
		//TODO: if the file is empty, return zero size list. if this is the case, ignore this
		//part of the filtering process, and let all files through.
		
		printToConsole(threadStamp, "readKeyWords function uninitialized. read 0 keywords");
		return new Vector<String>();		
	}
	/*================================================================================
	 * readProdList: compiles list of unique drugs
	 *===============================================================================*/
	protected static HashMap<String,Integer> readProdList(String threadStamp, 
	HashMap<String,Integer> genMap, HashMap<String,Integer> combIdMap) throws Exception{

		long startTime = System.currentTimeMillis();
		HashMap<String,Integer> prodMap = new HashMap<String,Integer>();

		//intialize statement
		String selectQueryProd = product_statements.Select(new ArrayList<>(
				Arrays.asList("name","id","combination_id","type")));
		
		//connect and execute statement/retreive data
		DBConnect con = new DBConnect();
		Main.connectToDatabase(con);
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
		
		printToConsole(threadStamp, "read " + prodMap.size() + " product names");			
		printToConsole(threadStamp, "run time:  " + 
				((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return prodMap;
	}
	/*================================================================================
	 * readQueryList: compiles list of queries to search for in articles
	 *===============================================================================*/
	protected static HashMap<String,Integer> readQueryList(String threadStamp) throws Exception{

		long startTime = System.currentTimeMillis();
		
		HashMap<String,Integer> returnMap = new HashMap<String,Integer>();
		
		//intialize statement
		String selectQueryProd = moreover_query_statements.Select(new ArrayList<>(
						Arrays.asList("id","query")));
		
		DBConnect con = new DBConnect();
		Main.connectToDatabase(con);
		ResultSet rs = con.ExecuteQuery(selectQueryProd);
		con.CommitClose();	
		
		//constuct string list from result set
		while (rs.next()) {
			String temp =  rs.getString("query");
			if (temp == null) {
				throw new Exception("null drugname found");
			} else {
				temp = temp.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
			}
			returnMap.put(temp, rs.getInt("id"));
		}
		
		printToConsole(threadStamp, "read " + returnMap.size() + " queries");			
		printToConsole(threadStamp, "run time:  " + 
				((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return returnMap;
	}
	/*================================================================================
	 * printToConsole: prints a string to the console, including thread identification
	 *===============================================================================*/
	protected static void printToConsole(String threadStamp, String statement) {
		System.out.println(threadStamp + statement);
	}

}

