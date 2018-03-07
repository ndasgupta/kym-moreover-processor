package Filters;

import java.util.HashMap;
import java.util.Set;

import dbconnect.general.document_relation_row;
import main.MoreoverArticle;

public class CombinationFilter extends Filter {

	private static HashMap<String, Integer> queryMap;
	
	@Override
	public void initialize(String threadStamp, boolean active) throws Exception {
		this.active = initializeSpecific(threadStamp, active);	
	}
	
	public void initialize(String threadStamp, HashMap<String, Integer> queryMap, boolean active) 
	throws Exception {
		this.active = active;
		if (!active) { return; }
		CombinationFilter.queryMap = queryMap;
		printToConsole(threadStamp, "combination ids found: " + queryMap.size());
	}

	@Override
	public boolean relevanceCheck(MoreoverArticle article) throws Exception {
		return false;
	}

	@Override
	public String populateArticle(MoreoverArticle article, Set<String> queriesFound) throws Exception {
		return null;
	}

	@Override
	public void populateRelation(String query, document_relation_row relation) throws Exception {
		if (!active) { return; }
		relation.combination_id = queryMap.get(query);
	}

}
