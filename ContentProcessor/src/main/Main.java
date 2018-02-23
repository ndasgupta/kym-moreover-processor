package main;

import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import dbconnect.general.moreover_sources_statements;
import dbconnect.general.product_statements;
import dbconnect.main.DBConnect;
import queue.moreover.MoreoverQueueOperator;
import xmlparser.AuxFileHandling.FieldChainInterpreter;
import xmlparser.Types.FieldChain;

/*
 * TODO: incorporate conditions in relevance check.
 * TODO: not currently set up to read all info. Check notes to complete this. (image/logo blobs)
 * TODO: check if any thread has been terminated (exception outside loop). restart it if so.
 * TODO: increase speed. content processor reading about 500 articles per minute, so
 * 		the queries must be set slower to compensate. should be at 500 articles every 12 seconds.
 * 		POSSIBLE SOLUTION: TODO: should be able to dequeue many at a time. 
 * 			this is a change that must be made in AzureStorageConnect in the database workspace.
 * TODO: logging/send emails on exceptions.
 * 		azure log management/log analytics. log4j.
*/
/*================================================================================
 *ContentProcessor
 *
 *This program creates a thread pool where each thread has a corresponding queue in
 *Azure Storage. Each thread continuously reads moreover articles from its assigned
 *queue and checks them against a set of criteria for relevance.
 *
 *There are two general areas where filtering takes place. The first is in this main
 *class, when reading in the master "generic names" list and "product list" from 
 *the database. In specifying which of these we want to look at, discretion is
 *applied. The second place is in the QueueReaderRunnable class, exclusively in the
 *checkRelevantArticle function.
 *===============================================================================*/

public class Main {

	public static final String mainThreadStamp = "(reader main) ";
	
	public static Queue<MoreoverArticle> relevantArticleQueue = new LinkedList<MoreoverArticle>();
	public static Semaphore relevantArticleQueueLock = new Semaphore(1,true);
	
	public static HashMap<String,Integer> combinationIdMap;
	public static HashMap<String,Integer> genericNameMap;
	public static HashMap<String,Integer> productMap;
	public static final List<String> categoryList = new  ArrayList<>(Arrays.asList(
			"academic",
			"general",
			"government",
			"journal",
			"local",
			"national",
			"organization"));
	public static final List<String> editorialRankList = new ArrayList<>(Arrays.asList(
			"1","2","3","4"));
	public static final List<FieldChain> chainList = (new FieldChainInterpreter()).GetChainsDirect(
			new ArrayList<>(Arrays.asList(
			"TEXT,sequenceId",
			"TEXT,title",
			"TEXT,content",
			"TEXT,publishedDate",
			"TEXT,estimatedPublishedDate",
			"TEXT,url",
			"TEXT,source,id",
			"TEXT,source,name",
			"TEXT,source,homeUrl",
			"TEXT,source,category",
			"TEXT,source,editorialRank",
			"TEXT,location,country"
	)));
	public static final String GEN_LIST_QUERY = "select distinct d.name, d.combination_id, d.id "
			+ "from product b, combination c, generic_name d "
			+ "where b.type != 'HUMAN OTC DRUG' "
			+ "and b.combination_id = c.id "
			+ "and d.combination_id = c.id";
	
	//fixed parameters
	public static final int QUEUECOUNT = 20;
	public static final int MILLIS_PER_SEC = 1000;
	public static final int MILLIS_PER_MINUTE = 60*MILLIS_PER_SEC;
	public static final int MILLIS_PER_HOUR = 60*MILLIS_PER_MINUTE;
	public static final int MILLIS_PER_DAY = 24*MILLIS_PER_HOUR;
	public static final int THREAD_SLEEP_TIME_MILLIS = 1*MILLIS_PER_MINUTE;
	public static final int THREAD_SLEEP_TIME_MILLIS_SHORT = 1000;
	
	public static long exceptionCountLast;
	public static long articleCountLast;
	public static long relevantArticleCountLast;
		
	/*================================================================================
	 *main
	 *===============================================================================*/
	public static void main(String[] args) {		
		
		//initialize map so all runnables can be accurately referenced
		HashMap<Integer,QueueReaderRunnable> runnableMap = 
				new HashMap<Integer,QueueReaderRunnable>();
		
		long startTime = System.currentTimeMillis();
		
		try {
			
			//populate information from database
			combinationIdMap = new HashMap<String,Integer>();
			genericNameMap = readGenList(combinationIdMap);
			productMap = readProdList(genericNameMap,combinationIdMap);
			printToConsole("combination ids found: " + combinationIdMap.size());
					
			//initialize and execute the queue readers, and store in map
			for (int i = 0; i < QUEUECOUNT; i++) {
				QueueReaderRunnable reader = executeQueueReader(i);
				runnableMap.put(i, reader);
			}
			
			//TODO: check if any thread has been terminated (exception outside loop). restart it if so.
			
			
			//every minute, evaluate status of QueueReader threads, and write all gathered 
			//information to database and azure cloud storage.
			WriterRunnable writer = new WriterRunnable();
			writer.threadCompleted = true;
			long localExceptionCount = 0;
			while (true) {
				Thread.sleep(THREAD_SLEEP_TIME_MILLIS);
				
				//check status of previous writer. wait until completion to run the next
				while (!(writer.threadCompleted || writer.exceptionFound)) {
					Thread.sleep(THREAD_SLEEP_TIME_MILLIS_SHORT);
				} if (writer.exceptionFound) { localExceptionCount++; }
				

				printThreadPoolStatus(runnableMap, startTime, localExceptionCount);
				
				writer = executeWriter();
			}
			
		} catch (Exception e) {
			printToConsole("MAIN THREAD EXCEPTION");
			e.printStackTrace();
		}
		
	}
	/*================================================================================
	 * connectToDatabase: function to be used by all classes in this program for database
	 * connection. makes sure database being accessed is consistent.
	 *===============================================================================*/
	protected static void connectToDatabase(DBConnect connect) throws Exception {
		connect.ConnectProd();
	}
	/*================================================================================
	 * executeQueuReader: initializes, executes and returns a QueueReaderRunnable
	 *===============================================================================*/
	protected static QueueReaderRunnable executeQueueReader(int queueNum) throws Exception {	
		QueueReaderRunnable reader = new QueueReaderRunnable();
		reader.queueNum = queueNum;
		reader.genericNameMap = genericNameMap;
		reader.productMap = productMap;
		reader.combinationIdMap = combinationIdMap;
		reader.categoryList = categoryList;
		reader.editorialRankList = editorialRankList;
		reader.chainList = chainList;

		reader.relevantArticleQueue = relevantArticleQueue;
		reader.relevantArticleQueueLock = relevantArticleQueueLock;
		(new Thread(reader)).start();
		return reader;
	}
	/*================================================================================
	 * executeWriter: initializes, executes and returns a WriterRunnable
	 *===============================================================================*/
	protected static WriterRunnable executeWriter() throws Exception {
		
		//dequeue up to 100 relevant articles and put into a list
		List<MoreoverArticle> writeList = new Vector<MoreoverArticle>();
		relevantArticleQueueLock.acquire();
		for (int i = 0; i < WriterRunnable.WRITE_LIMIT; i++) {
			MoreoverArticle nextArticle = relevantArticleQueue.poll();
			if (nextArticle==null) {
				break;
			}
			writeList.add(nextArticle);
		}
		relevantArticleQueueLock.release();
		
		//initialize and execute writer
		WriterRunnable writer = new WriterRunnable();
		writer.writeList = writeList;
		writer.genericNameMap = genericNameMap;
		writer.productMap = productMap;
		writer.combinationIdMap = combinationIdMap;
		Thread writerThread = new Thread(writer);
		writerThread.start();
		return writer;
	}
	/*================================================================================
	 * getCurrentRunTime
	 *===============================================================================*/
	protected static long getCurrentRunTime(long startTime) {
		return System.currentTimeMillis() - startTime;
	}
	/*================================================================================
	 * printCurrentRunTime: given start time, prints current run time in seconds
	 *===============================================================================*/
	protected static long printCurrentRunTime(long startTime, String text) {
		DecimalFormat df = new DecimalFormat("#.00");
		long runTimeMillis = System.currentTimeMillis() - startTime;
		long runTime;
		String unit;
		if (runTimeMillis < 2*MILLIS_PER_MINUTE) {
			runTime = runTimeMillis/MILLIS_PER_SEC;
			unit = "sec";
		} else if (runTimeMillis < 2*MILLIS_PER_HOUR) {
			runTime = runTimeMillis/MILLIS_PER_MINUTE;
			unit = "min";
		} else if (runTimeMillis < 3*MILLIS_PER_DAY) {
			runTime = runTimeMillis/MILLIS_PER_HOUR;
			unit = "hrs";
		} else {
			runTime = runTimeMillis/MILLIS_PER_SEC;
			unit = "days";
		}		
		String runTimeString = df.format(runTime);
		printToConsole(text + ": " + runTimeString + " " + unit);
		return runTime;
	}
	/*================================================================================
	 * printThreadPoolStatus: aggregates and prints thread pool statistics
	 *===============================================================================*/
	protected static void printThreadPoolStatus(HashMap<Integer, QueueReaderRunnable> runnableMap,
	long startTime, long localExceptionCount) throws Exception {
		long exceptionCountSum = localExceptionCount;
		long articleCountSum = 0;
		long relevantArticleCountSum = 0;
		for (QueueReaderRunnable r: runnableMap.values()) {
			exceptionCountSum+=r.exceptionCount;
			articleCountSum+=r.articleCount;
			relevantArticleCountSum+=r.relevantArticleCount;
		}
		printCurrentRunTime(startTime, "current run time");
		printToConsole("total article count: " + articleCountSum);
		printToConsole("total relevant article count: " + relevantArticleCountSum);
		printToConsole("articles since last report: " + (articleCountSum - articleCountLast));
		printToConsole("current relevantArticleQueue size: " + relevantArticleQueue.size());
		printToConsole("exception count: " + exceptionCountSum);
		
		articleCountLast = articleCountSum;
		exceptionCountLast = exceptionCountSum;
		relevantArticleCountLast = relevantArticleCountSum;
	}
	/*================================================================================
	 * printToConsole: prints a string to the console, including thread identification
	 *===============================================================================*/
	protected static void printToConsole(String statement) {
		System.out.println(mainThreadStamp + statement);
	}
	/*================================================================================
	 * readGenList: compiles list of unique drugs
	 *===============================================================================*/
	protected static HashMap<String,Integer> readGenList(HashMap<String,Integer> combIdMap) 
			throws Exception{

		long startTime = System.currentTimeMillis();
		
		HashMap<String,Integer> genMap = new HashMap<String,Integer>();
		
		DBConnect con = new DBConnect();
		connectToDatabase(con);
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
		
		printToConsole("read " + genMap.size() + " generic names");			
		printToConsole("run time:  " + ((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return genMap;
	}
	/*================================================================================
	 * readProdList: compiles list of unique drugs
	 *===============================================================================*/
	protected static HashMap<String,Integer> readProdList(HashMap<String,Integer> genMap, 
			HashMap<String,Integer> combIdMap) throws Exception{

		long startTime = System.currentTimeMillis();
		HashMap<String,Integer> prodMap = new HashMap<String,Integer>();

		//intialize statement
		String selectQueryProd = product_statements.Select(new ArrayList<>(
				Arrays.asList("name","id","combination_id","type")));
		
		//connect and execute statement/retreive data
		DBConnect con = new DBConnect();
		connectToDatabase(con);
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
		
		printToConsole("read " + prodMap.size() + " product names");			
		printToConsole("run time:  " + ((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return prodMap;
	}
	/*================================================================================
	 * readSources: compiles list of relevant sources from moreover_sources, name and
	 * sourceid columns
	 *===============================================================================*/
	protected static HashMap<Long, String> readSources() throws Exception{

		long startTime = System.currentTimeMillis();
		
		HashMap<Long, String> sourceMap = new HashMap<Long, String>();
		
		//construct and execute select statement for druglist
		String selectQuery = moreover_sources_statements.Select(new ArrayList<>(
				Arrays.asList("sourceid","name")));
		DBConnect con = new DBConnect();
		connectToDatabase(con);
		ResultSet rs = con.ExecuteQuery(selectQuery);
		con.CommitClose();	
		
		//constuct string list from result set
		while (rs.next()) {
			String temp_sourceName =  rs.getString("name");
			Long temp_sourceId = rs.getLong("sourceid");
			if (temp_sourceName == null) {
				throw new Exception("null source name found");
			} else if (temp_sourceId == 0) {
				throw new Exception("null source id found");
			}
			sourceMap.put(temp_sourceId, temp_sourceName);
		}
		
		printToConsole("read " + sourceMap.size() + " sources from moreover_sources table");			
		printToConsole("run time:  " + ((System.currentTimeMillis() - startTime)/1000) + " sec");	
		
		return sourceMap;
	}
	
	/*================================================================================
	 * TESTING/DEBUGGING FUNCTIONS ***************************************************
	 *===============================================================================*/
	/*================================================================================
	 * printQueuesizes: prints sizes of all queues
	 *===============================================================================*/
	protected static void printQueueSizes() {
		MoreoverQueueOperator queueOp = new MoreoverQueueOperator();
		for (int i = 0; i < QUEUECOUNT; i++) {
			String queueName = QueueReaderRunnable.QUEUE_PREFIX + i;
			try {
				queueOp.connectQueue(queueName);
				long queueSize = queueOp.getQueueLength(queueName);
				System.out.println(queueName + ": " + queueSize);
			} catch (Exception e) {
				System.out.println(queueName + ": EXCEPTION (" + e.getMessage() + ")");
			}
		}
	}
	
}
