package Filters;

import java.util.Set;

import dbconnect.general.document_relation_row;
import main.MoreoverArticle;

public abstract class Filter {

	public static final int MIN_NAME_LENGTH = 3;
	public static final int SNIPPET_LENGTH = 1000;
	
	public boolean active = false;
	
	/*================================================================================
	 * initialize: instantiates and populates data structures used for filtering
	 *===============================================================================*/
	public abstract void initialize(String threadStamp, boolean active) throws Exception;
	
	/*================================================================================
	 * relevanceCheck: checks an unverified article for relevance
	 *===============================================================================*/
	public abstract boolean relevanceCheck(MoreoverArticle article) throws Exception;
	
	/*================================================================================
	 * populateArticle: populates a verified article with information
	 *===============================================================================*/
	public abstract String populateArticle(MoreoverArticle article, Set<String> queriesFound) 
	throws Exception;
	
	/*================================================================================
	 * populateRelation: populates a database relation row with relevant information
	 *===============================================================================*/
	public abstract void populateRelation(String query, document_relation_row relation) 
	throws Exception;
	
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
	 * initializeSpecific: to be used to override initialize if alternate initiliaze
	 * function is used by the subclass
	 *===============================================================================*/
	public static boolean initializeSpecific(String threadStamp, boolean active) throws Exception {
		if (!active) { return false; }
		throw new Exception("must use class specific initalize function");
	}
	
	/*================================================================================
	 * printToConsole: prints a string to the console, including thread identification
	 *===============================================================================*/
	protected static void printToConsole(String threadStamp, String statement) {
		System.out.println(threadStamp + statement);
	}
	
}
