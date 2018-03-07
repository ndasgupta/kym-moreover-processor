package Filters;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Set;

import dbconnect.general.document_relation_row;
import dbconnect.main.DBConnect;
import main.Main;
import main.MoreoverArticle;

/*================================================================================
 * GenericNamesFilter
 * 
 * filters using generic drug names from the database.
 *===============================================================================*/
public final class GenericNamesFilter extends Filter {

	private static HashMap<String,Integer> queryMap;
	public static final String GEN_LIST_QUERY = "select distinct d.name, d.combination_id, d.id "
			+ "from product b, combination c, generic_name d "
			+ "where b.type != 'HUMAN OTC DRUG' "
			+ "and b.combination_id = c.id "
			+ "and d.combination_id = c.id";
	
	
	@Override
	public void initialize(String threadStamp, boolean active) throws Exception {
		this.active = initializeSpecific(threadStamp, active);	
	}
	
	public void initialize(String threadStamp, HashMap<String,Integer> combIdMap, boolean active) 
	throws Exception{

		this.active = active;
		if (!active) { return; }
		
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
		
		queryMap = genMap;
		
	}

	public HashMap<String, Integer> getMap() {
		if (!active) { return new HashMap<String, Integer>(); }
		else return queryMap;
	}
	
	@Override
	public boolean relevanceCheck(MoreoverArticle article) throws Exception {
		
		if (!active) { return false; }
		
		for (String drug: queryMap.keySet()) {
			if (doesMatch(article.title, drug) || doesMatch(article.content, drug)){
				article.declareRelevant(drug);
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
		relation.generic_name_id = queryMap.get(query);
	}

}
