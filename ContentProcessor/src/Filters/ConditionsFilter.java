package Filters;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import dbconnect.general.condition_statements;
import dbconnect.general.document_relation_row;
import dbconnect.main.DBConnect;
import main.Main;
import main.MoreoverArticle;

public class ConditionsFilter extends Filter {

	private static HashMap<String,Integer> queryMap;
	
	@Override
	public void initialize(String threadStamp, boolean active) throws Exception {
		
		this.active = active;
		if (!active) { return; }
		
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
		
		queryMap = condMap;
		
	}

	@Override
	public boolean relevanceCheck(MoreoverArticle article) throws Exception {
		return false;
	}

	@Override
	public String populateArticle(MoreoverArticle article, Set<String> queriesFound) throws Exception {
		if (!active) { return null; }
		for (String q: queryMap.keySet()) {
			if (doesMatch(article.title, q) || doesMatch(article.content, q)) {
				queriesFound.add(q);
			}
		}
		return null;
	}

	@Override
	public void populateRelation(String query, document_relation_row relation) throws Exception {
		if (!active) { return; }
		relation.condition_id = queryMap.get(query);
	}

}
