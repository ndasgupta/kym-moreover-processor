package main;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Semaphore;

import Filters.FilterOperator;
import queue.moreover.MoreoverQueueOperator;
import xmlparser.Operations.XMLOperator;
import xmlparser.Types.FieldChain;
import xmlparser.Types.XMLNode;


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
	public List<FieldChain> chainList = null;
	public FieldChain imageUrlKeyChain = null;
	public FieldChain urlKeyChain = null;
	public Exception exc = null;	
	public FilterOperator filter = null;	
	
	//return values
	public long exceptionCount = 0;
	public long articleCount = 0;
	public long relevantArticleCount = 0;
	
	//fixed parameters
	private static final int MIN_QUEUE_SIZE = 400;
	private static final int SHORT_SLEEP_TIME_MILLIS = 2000;
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
				
				while (queueOp.getQueueLength(queueName) < MIN_QUEUE_SIZE) {				
					Thread.sleep(SHORT_SLEEP_TIME_MILLIS);
				}
				
				//dequeue next message
				String nextArticle;
				try { nextArticle = queueOp.dequeue(queueName); }
				catch (NullPointerException npe) { nextArticle = null; }
				if (nextArticle == null) {					
					continue;
				}

				articleCount++;
			
				//extract data and check if relevant
				MoreoverArticle row = extractData(nextArticle);
				
				//if relevant, write to final queue
				if (filter.checkRelevantArticle(threadStamp, row)) {				
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
		if (filter == null) {
			throw new Exception("filter not initialized. value is null (" + queueNum + ")");
		} else if (chainList == null || imageUrlKeyChain == null || urlKeyChain == null) {
			throw new Exception("fieldChains not initialized. value is null (" + queueNum + ")");
		} else if (relevantArticleQueue == null || relevantArticleQueueLock == null) {
			throw new Exception("relevantArticleQueue values not initialized. "
					+ "value is null (" + queueNum + ")");
		} 
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
		
		//retreive specifically referenced keychains.
		try { row.url = nodeMap.get(urlKeyChain).get(0).innerXml.trim(); }
		catch (NullPointerException e) { row.url = null; }
		try { row.imageUrl = nodeMap.get(imageUrlKeyChain).get(0).innerXml.trim(); }
		catch (NullPointerException e) { row.imageUrl = null; }
		
		HashMap<String, String> dataMap = parser.IdentifyAllMap(nodeMap);
		row.sequenceId = Long.parseLong(dataMap.get("sequenceId"));
		row.title = dataMap.get("title");
		row.content = dataMap.get("content");
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
		row.sourceUrl = dataMap.get("homeUrl");
		
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
