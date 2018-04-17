package Filters;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import dbconnect.general.document_relation_row;
import dbconnect.general.product_statements;
import dbconnect.main.DBConnect;
import main.Main;
import main.MoreoverArticle;

public class ProductsFilter extends Filter {
	
	private static HashMap<String,Integer> queryMap;
	
	@Override
	public void initialize(String threadStamp, boolean active) throws Exception {
		this.active = initializeSpecific(threadStamp, active);	
	}

	public void initialize(String threadStamp, HashMap<String,Integer> combIdMap, 
	boolean active) throws Exception{

		this.active = active;
		if (!active) { return; }
		
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
			if (!(rsProd.getString("type").trim().equalsIgnoreCase("HUMAN PRESCRIPTION DRUG"))) {
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
		
		
		queryMap = prodMap;
		
	}
	
	@Override
	public boolean relevanceCheck(MoreoverArticle article) throws Exception {
		
		if (!active) { return false; }
		
		for (String q: queryMap.keySet()) {
			if (doesMatch(article.title, q) || doesMatch(article.content, q)){
				article.declareRelevant(q);
				return true;
			}
		}
		return false;
	}

	@Override
	public String populateArticle(MoreoverArticle article, Set<String> queriesFound) throws Exception {
		
		if (!active) { return null; }
		
		String relevanceValue = null;
		for (String q: queryMap.keySet()) {
			if (doesMatch(article.title, q)) {
				queriesFound.add(q);
				relevanceValue = MoreoverArticle.RELEVANCY_TITLE_VAL;
			} else if (doesMatch(article.content, q)) {
				queriesFound.add(q);
				if (relevanceValue == null) { relevanceValue = MoreoverArticle.RELEVANCY_CONTENT_VAL; }
			}
		}
		return relevanceValue;
	}

	@Override
	public void populateRelation(String query, document_relation_row relation) throws Exception {
		if (!active) { return; }
		relation.product_id = queryMap.get(query);
	}
	
	public HashMap<String,Integer> getMap() {
		return queryMap;
	}

}
