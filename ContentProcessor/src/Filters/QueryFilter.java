package Filters;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import dbconnect.general.document_relation_row;
import dbconnect.general.moreover_query_statements;
import dbconnect.main.DBConnect;
import main.Main;
import main.MoreoverArticle;

public class QueryFilter extends Filter {

	private static HashMap<String,Integer> queryMap;
	
	@Override
	public void initialize(String threadStamp, boolean active) throws Exception {
		
		this.active = active;
		if (!active) { return; }
		
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
		
		queryMap = returnMap;
	}

	@Override
	public boolean relevanceCheck(MoreoverArticle article) throws Exception {
		
		if (!active) { return false; }
		
		for (String q: queryMap.keySet()) {
			if (doesMatch(article.title, q) || doesMatch(article.content, q)) {
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
		relation.query_id = queryMap.get(query);
	}
	
	public HashMap<String,Integer> getMap() {
		return queryMap;
	}

}
