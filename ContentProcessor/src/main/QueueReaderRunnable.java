package main;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;

import blob.moreover.MoreoverBlobOperator;
import queue.moreover.MoreoverQueueOperator;
import xmlparser.AuxFileHandling.FieldChainInterpreter;
import xmlparser.Operations.XMLOperator;
import xmlparser.Types.FieldChain;
import xmlparser.Types.XMLNode;

/*TODO: should be able to dequeue many at a time. this is a change that must be made in
		AzureStorageConnect in the database workspace.
*/
/*================================================================================
 * QueueReaderRunnable
 * 
 * this class is designed to run indefinitely.
 * 
 * The first try block initializes the queue connection Upon success of that block, the 
 * thread enters an infinite loop. With each iteration, the thread dequeues an article 
 * from  its assigned queue, checks it for relevence, and stores it the global relevant 
 * articles holder.
 * 
 * Note: a single QueueReaderRunnable is assigned to a specific queue in the azure 
 * cloud (denoted by queueNum). The code here and in the QueueOperator are designed 
 * without protection against concurrency in accessing a single queue. So, no two 
 * concurrent threads can operate on the same queue.
 *===============================================================================*/
public class QueueReaderRunnable implements Runnable {
		
	//parameters
	public int queueNum = -1;
	public Queue<MoreoverArticle> relevantArticleQueue = null;
	public Semaphore relevantArticleQueueLock = null;
	public HashMap<String,Integer> genericNameMap = null;
	public HashMap<String,Integer> productMap = null;
	public HashMap<String,Integer> combinationIdMap = null;
	public List<String> categoryList = null;
	public List<String> editorialRankList = null;
	public List<FieldChain> chainList = null;
	public Exception exc = null;
	
	//return values
	public long exceptionCount = 0;
	public long articleCount = 0;
	public long relevantArticleCount = 0;
	
	//fixed parameters
	private static final int SLEEP_TIME_MILLIS = 2000;
	private static final int MIN_DRUGNAME_LENGTH = 3;
	public static final String QUEUE_PREFIX = "moreover-queue-";

		
	public String threadStamp;
	/*================================================================================
	 * run
	 *===============================================================================*/
	@Override
	public void run() {
		
		MoreoverQueueOperator queueOp = new MoreoverQueueOperator();
		String queueName = QUEUE_PREFIX + queueNum;
		boolean terminate = false;
		
		try {				
			checkParameters();
			printToConsole("running queue reader");		
			queueOp.connectQueue(queueName);
		} catch(Exception e) {
			exc = e;
			e.printStackTrace();
			terminate = true;
		}
		
		//continuously dequeue and process messages
		while (!terminate) {
			try {
				//dequeue next message
				String nextArticle = null;
				try  { nextArticle = queueOp.dequeue(queueName); }
				catch (NullPointerException ne) {
					Thread.sleep(SLEEP_TIME_MILLIS);
					continue;
				}
				articleCount++;
				
				//extract data and check if relevant
				MoreoverArticle row = extractData(nextArticle);
				
				//if relevant, write to final queue
				if (checkRelevantArticle(row)) {				
					printToConsole("RELEVANT ARTICLE FOUND (" + row.firstDrugFound + ")");
					enqueueRelevantArticle(row);
					relevantArticleCount++;
				}
				
				//delete message from queue
				queueOp.deleteLast(queueName);
			}
			catch (Exception e) {
				//Exceptions here:
				printToConsole("exception: " + e.getMessage());
				e.printStackTrace();
				try { queueOp.deleteLast(queueName); }
				catch (Exception e1) { printToConsole("exception: failed to delete last queue item"); }
				exceptionCount+=1;
			}
		}
					
		printToConsole("thread terminated");
	}	
	/*================================================================================
	 * checkParameters: checks that parameters have been properly initialized and throws
	 * exception if not.
	 *===============================================================================*/
	public void checkParameters() throws Exception {
		if (queueNum >= Main.QUEUECOUNT || queueNum < 0) { 
			throw new Exception("queueNum not specified, "
					+ "or invalid queueNum specified (" + queueNum + ")");
		} 
		threadStamp = "(reader queue_" + queueNum + ") ";
		if (genericNameMap == null || productMap == null || combinationIdMap == null) {
			throw new Exception("drug lists not initialized. value is null (" + queueNum + ")");
		} else if (categoryList == null || editorialRankList == null) {
			throw new Exception("sources not initialized. value is null (" + queueNum + ")");
		} else if (chainList == null) {
			throw new Exception("chainList not initialized. value is null (" + queueNum + ")");
		}else if (relevantArticleQueue == null || relevantArticleQueueLock == null) {
			throw new Exception("relevantArticleQueue values not initialized. "
					+ "value is null (" + queueNum + ")");
		}
	}
	/*================================================================================
	 * checkRelevantArticle: checks whether or not article is desired/relevant. If
	 * relevant, update the article to represent relevance values, and return true.
	 *===============================================================================*/
	protected boolean checkRelevantArticle(MoreoverArticle article) throws Exception {		
				
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
		
		//do a cursory check to determine whether or not the article has drug information
		boolean isRelevant = false;
		String firstDrugFound = null;
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
		
		//if the article is determined to be relevant, do a full check, and modify the moreoever article
		//to hold the results.
		if (isRelevant) {
			Set<String> namesFound = new HashSet<String>();
			String relevanceValue = null;
			for (String g: genericNameMap.keySet()) {
				if (doesMatch(article.title, g) || doesMatch(article.content, g)) {
					namesFound.add(g);
					if (relevanceValue == null) { relevanceValue = MoreoverArticle.RELEVANCY_TITLE_VAL; }
				}
			}
			for (String p: productMap.keySet()) {
				if (doesMatch(article.title, p) || doesMatch(article.content, p)) {
					namesFound.add(p);
					if (relevanceValue == null) { relevanceValue = MoreoverArticle.RELEVANCY_CONTENT_VAL; }
				}
			}			
			article.declareRelevant(namesFound, relevanceValue, firstDrugFound);			
		}		
		return isRelevant;
	}
	
	/*================================================================================
	 * doesMatch: checks a body of text 'content' for appearance of a string 'query'
	 *===============================================================================*/
	public static boolean doesMatch(String content, String query) {
		
		//return if the query is too short.
		if( query.replaceAll("[^a-zA-Z0-9]","").trim().length() <= MIN_DRUGNAME_LENGTH){
			return false;
		}		
		content = content.length() >= 1000 ? content.substring(0, 999) : content;
		content = content.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
		
		query = query.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
		if (content.contains(query)) {
			return true ;
		}
		return false ;
	}
	/*================================================================================
	 * enqueueRelevantArticle: enqueues a relevant article in the relevantArticleQueue,
	 * using the global semaphore to ensure safe concurrency.
	 *===============================================================================*/
	protected void enqueueRelevantArticle(MoreoverArticle article) throws Exception{
		relevantArticleQueueLock.acquire();
		relevantArticleQueue.add(article);
		relevantArticleQueueLock.release();
	}
	/*================================================================================
	 * extractData: given an xml article as a string, parses and returns a MoreoverArticle
	 * object.
	 *===============================================================================*/
	protected MoreoverArticle extractData(String articleXml) throws Exception {
		
		XMLOperator parser = new XMLOperator();
		MoreoverArticle row = new MoreoverArticle();
		
		HashMap<FieldChain, List<XMLNode>> nodeMap = parser.FieldChainParseString(
				articleXml, chainList); 

		HashMap<String, String> dataMap = parser.IdentifyAllMap(nodeMap);
		row.sequenceId = Long.parseLong(dataMap.get("sequenceId"));
		row.title = dataMap.get("title");
		row.content = dataMap.get("content");
		row.url = dataMap.get("url");
		try { 
			row.sourceId = Integer.parseInt(dataMap.get("id")); 
		} catch (Exception e) { row.sourceId = 0; }
		row.sourceName = dataMap.get("name");
		row.sourceUrl = dataMap.get("homeUrl");
		row.country = dataMap.get("country");
		//try to extract publish date.
		try { 
			row.publishDate = new Timestamp((new SimpleDateFormat("yyyy-MM-dd")).parse(
					dataMap.get("publishedDate").substring(0,9)).getTime());
		} catch (Exception e) { 
			try { 
				row.publishDate = new Timestamp((new SimpleDateFormat("yyyy-MM-dd")).parse(
						dataMap.get("estimatedPublishedDate").substring(0,9)).getTime());
			} catch (Exception e1) { row.publishDate = null; }		
		}
		row.recordDate = new Timestamp(new Date().getTime());
		row.category = dataMap.get("category");
		row.editorialRank = dataMap.get("editorialRank");
		
		row.fullXml = articleXml;
		
		return row;
	}
	/*================================================================================
	 * printToConsole: prints a string to the console, including thread identification
	 *===============================================================================*/
	protected void printToConsole(String statement) {
		System.out.println(threadStamp + statement);
	}
	
	
}
