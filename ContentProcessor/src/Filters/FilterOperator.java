package Filters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import dbconnect.general.document_relation_row;
import main.MoreoverArticle;

/*================================================================================
 *Filter
 *
 *Contains all functions and data structures for filtering relevant articles, 
 *decoupling this process from the rest of the program. Filtering phases:
 *
 *init
 *checkRelevantArticle
 *populateRelevantArticle
 *initRelationsRows
 *===============================================================================*/
public class FilterOperator {
	
	private CategoryFilter categoryFilter = new CategoryFilter();
	private EditorialRankFilter editorialRankFilter = new EditorialRankFilter();
	
	private CombinationFilter combinationFilter = new CombinationFilter();
	private ConditionsFilter conditionsFilter = new ConditionsFilter();
	private GenericNamesFilter genericNamesFilter = new GenericNamesFilter();
	private KeywordFilter keywordFilter = new KeywordFilter();
	private ProductsFilter productsFilter = new ProductsFilter();
	private QueryFilter queryFilter = new QueryFilter();
	
	public static final int SNIPPET_LENGTH = 1000;
	public static final int MIN_NAME_LENGTH = 3;
	
	public boolean initialized = true;
	
	/*================================================================================
	 * init: initialize all of the filter values.
	 *===============================================================================*/
	public void init(String threadStamp, int matchThreshold) throws Exception {
		
		HashMap<String, Integer> combIdMap = new HashMap<String,Integer>();

		categoryFilter.initialize(threadStamp, true);
				
		editorialRankFilter.initialize(threadStamp, true);
		
		genericNamesFilter.initialize(threadStamp, combIdMap, false);
		
		productsFilter.initialize(threadStamp, genericNamesFilter.getMap(), combIdMap, false);
		
		combinationFilter.initialize(threadStamp, combIdMap, false);
		
		conditionsFilter.initialize(threadStamp, true);
		
		keywordFilter.initialize(threadStamp, matchThreshold, true);
		
		queryFilter.initialize(threadStamp, true);
		
		initialized = true;
	}
	/*================================================================================
	 * checkRelevantArticle: checks whether or not article is desired/relevant. If
	 * relevant, update the article to represent relevance values, and return true.
	 *===============================================================================*/
	public boolean checkRelevantArticle(String threadStamp, MoreoverArticle article) 
	throws Exception {		
		
		if (!initialized) { throw new Exception("filters not initialized"); }
		
		//FILTER 1: check that the article source is relevant.
		if (!categoryFilter.relevanceCheck(article)) { return false; }
				
		if (!editorialRankFilter.relevanceCheck(article)) { return false; }
						
		//FILTER 2: horizontal keyword search.
		if (!keywordFilter.relevanceCheck(article)) { return false; }
		
		//FILTER 3: do a cursory check to determine whether or not the article has drug information		
		if (queryFilter.relevanceCheck(article)) { return true; }
		
		if (genericNamesFilter.relevanceCheck(article)) { return true; }
		
		if (productsFilter.relevanceCheck(article)) { return true; }
		
		return false;	
	}
	
	/*================================================================================
	 * initRelationsRows: intializes a database relation row
	 *===============================================================================*/
	public List<document_relation_row> initRelationsRows(MoreoverArticle article) 
	throws Exception {
		
		if (!initialized) { throw new Exception("filters not initialized"); }
		
		List<document_relation_row> relations = new Vector<document_relation_row>();
		for (String s: article.drugsFound) {
			document_relation_row docRel = new document_relation_row();
			
			queryFilter.populateRelation(s, docRel);
			
			genericNamesFilter.populateRelation(s, docRel);
			
			productsFilter.populateRelation(s, docRel);
			
			combinationFilter.populateRelation(s, docRel);
			
			relations.add(docRel);
		}
		for (String s: article.conditionsFound) {
			document_relation_row docRel = new document_relation_row();
			
			conditionsFilter.populateRelation(s, docRel);
			
			relations.add(docRel);
		}
		return relations;
	}
	/*================================================================================
	 * populateRelevantArticle: uses filters to populate article with relevant data
	 * that isn't already included.
	 *===============================================================================*/
	public void populateRelevantArticle(MoreoverArticle article) throws Exception {
		
		if (!initialized) { throw new Exception("filters not initialized"); }
		
		Set<String> namesFound = new HashSet<String>();
		Set<String> conditionsFound = new HashSet<String>();
		String relevanceValue = null;
		
		queryFilter.populateArticle(article, namesFound);
		
		relevanceValue = genericNamesFilter.populateArticle(article, namesFound);
		
		String productRelevanceValue = productsFilter.populateArticle(article, namesFound);
		
		conditionsFilter.populateArticle(article, conditionsFound);
		
		
		if (relevanceValue != MoreoverArticle.RELEVANCY_TITLE_VAL) {
			relevanceValue = productRelevanceValue;
		}
		article.drugsFound = namesFound;
		article.relevanceValue = relevanceValue;
		article.conditionsFound = conditionsFound;
		
	}
	/*================================================================================
	 * printToConsole: prints a string to the console, including thread identification
	 *===============================================================================*/
	protected static void printToConsole(String threadStamp, String statement) {
		System.out.println(threadStamp + statement);
	}

}

