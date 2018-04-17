package main;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import Filters.FilterOperator;
import blob.moreover.MoreoverBlobOperator;
import dbconnect.general.document_attributes_statements;
import dbconnect.general.document_relation_statements;
import dbconnect.general.document_statements;
import dbconnect.main.DBConnect;
import queue.moreover.MoreoverQueueOperator;
import xmlparser.AuxFileHandling.FieldChainInterpreter;
import xmlparser.Types.FieldChain;


//TODO: side effect checks.
//TODO: reread databse tables every hour or so.
/*
 * TODO: change DB connection to local for server version.
 * TODO: logging/send emails on exceptions.
 * 		azure log management/log analytics. log4j.
 * TODO: blobUrls aren't actually working. look into this.
 * TODO: when filtering, check words that appear surrounded by spaces/punctuation, so that we aren't
 * 		reading instances where the keyword is part of a bigger word.
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
 *
 *known edge cases:
 *
 *1. queueReader hits end of queue while querier is entering into queue, resulting
 *		in a possible split message misalignment.
 *===============================================================================*/

public class Main {
	
	//thread identifier
	public static final String mainThreadStamp = "(reader main) ";
	
	public static Queue<MoreoverArticle> relevantArticleQueue = new LinkedList<MoreoverArticle>();
	public static Semaphore relevantArticleQueueLock = new Semaphore(1,true);	
	
	public static FilterOperator filter = new FilterOperator();
	
	public static final List<FieldChain> chainList = (new FieldChainInterpreter()).GetChainsDirect(
			new ArrayList<>(Arrays.asList( 
			"TEXT,content",
			"TEXT,estimatedPublishedDate",
			"TEXT,location,country",
			"TEXT,publishedDate",
			"TEXT,sequenceId",
			"TEXT,source,category",
			"TEXT,source,editorialRank",
			"TEXT,source,feed,imageUrl",
			"TEXT,source,homeUrl",
			"TEXT,source,id",
			"TEXT,source,name",
			"TEXT,title"
	)));
	public static FieldChain imageUrlChain;
	public static FieldChain urlChain;
	
	//fixed parameters
	public static final int QUEUECOUNT = 20;
	public static final int MILLIS_PER_SEC = 1000;
	public static final int MILLIS_PER_MIN = 60*MILLIS_PER_SEC;
	public static final int MILLIS_PER_HOUR = 60*MILLIS_PER_MIN;
	public static final int MILLIS_PER_DAY = 24*MILLIS_PER_HOUR;
	public static final int THREAD_SLEEP_TIME_MILLIS = 1*MILLIS_PER_MIN;
	public static final int THREAD_SLEEP_TIME_MILLIS_SHORT = 1000;
	
	public static long exceptionCountLast;
	public static long articleCountLast;
	public static long relevantArticleCountLast;
		
	/*================================================================================
	 *main
	 *===============================================================================*/
	public static void main(String[] args) {		
		
		//if (printQueueSizes()) { return; }
		
		int keyWordMatchThreshold = 3;
		if (args.length > 0) {
			try { keyWordMatchThreshold = Integer.parseInt(args[0]); }
			catch (Exception e) { 
				printToConsole("invalid argument. defaulting keyword match threshold to 3"); 
			}
		}
		
		//initialize map so all runnables can be accurately referenced
		HashMap<Integer,QueueReaderRunnable> runnableMap = 
				new HashMap<Integer,QueueReaderRunnable>();
		HashMap<Integer,Thread> threadMap = new HashMap<Integer, Thread>();
				
		long startTime = System.currentTimeMillis();
		
		try {
			
			//add chains to list that need to be specifically referenced later on. this is required
			//becuse they have the same field name ('url'), so the regular map method of retreiving
			//the data won't work.
			imageUrlChain = (new FieldChainInterpreter()).GetSingleFieldChain("TEXT,media,image,url");
			urlChain = (new FieldChainInterpreter()).GetSingleFieldChain("TEXT,url");
			chainList.add(imageUrlChain);
			chainList.add(urlChain);
			
			filter.init(mainThreadStamp, keyWordMatchThreshold);
			
			//initialize and execute the queue readers, and store in map
			for (int i = 0; i < QUEUECOUNT; i++) {
				executeQueueReader(i, runnableMap, threadMap);
			}
						
			//every minute, evaluate status of QueueReader threads, and write all gathered 
			//information to database and azure cloud storage.
			WriterRunnable writer = new WriterRunnable();
			writer.threadCompleted = true;
			long localExceptionCount = 0;
			while (true) {
				long startWait = System.currentTimeMillis();
				
				//check status of previous writer. wait until completion to run the next
				while (!(writer.threadCompleted || writer.exceptionFound)) {
					Thread.sleep(THREAD_SLEEP_TIME_MILLIS_SHORT);
				} if (writer.exceptionFound) { localExceptionCount++; }
				
				//once previous is complete, wait prescribed time, unless the queue is very full.
				if (Main.getCurrentRunTime(startWait) < THREAD_SLEEP_TIME_MILLIS ||
						relevantArticleQueue.size() < WriterRunnable.WRITE_LIMIT) {
					int waitTime = THREAD_SLEEP_TIME_MILLIS - (int) Main.getCurrentRunTime(startWait);
					if (waitTime > 0) {
						Thread.sleep(waitTime); 
					}
				}
				
				printThreadPoolStatus(runnableMap, startTime, localExceptionCount);
				
				//check for dead threads, and re-execute if any are found.
				Set<Integer> deadList = checkDeadThreads(threadMap);
				for (Integer i: deadList) {
					executeQueueReader(i,runnableMap,threadMap);
				}
				
				writer = executeWriter();
			}
			
		} catch (Exception e) {
			printToConsole("MAIN THREAD EXCEPTION");
			e.printStackTrace();
		}
		
	}
	/*================================================================================
	 * checkDeadThreads: checks for dead threads, and returns numbers
	 *===============================================================================*/
	public static Set<Integer> checkDeadThreads(HashMap<Integer,Thread> threadMap) 
	throws Exception {
		Set<Integer> deadList = new HashSet<Integer>();
		for (Integer i: threadMap.keySet()) {
			if (!threadMap.get(i).isAlive()) {
				deadList.add(i);
				printToConsole("found dead thread: " + i);
			}
		}
		return deadList;
	}
	/*================================================================================
	 * connectToDatabase: function to be used by all classes in this program for database
	 * connection. makes sure database being accessed is consistent.
	 *===============================================================================*/
	public static void connectToDatabase(DBConnect connect) throws Exception {
		connect.ConnectProd();
	}
	/*================================================================================
	 * executeQueuReader: initializes and executes a queue reader, recording the runnable
	 * and thread objects in the argument maps.
	 *===============================================================================*/
	protected static void executeQueueReader(int queueNum, 
	HashMap<Integer,QueueReaderRunnable> runnableMap, HashMap<Integer,Thread> threadMap) 
	throws Exception {	
		QueueReaderRunnable reader = new QueueReaderRunnable();
		reader.queueNum = queueNum;
		reader.filter = filter;
		reader.chainList = chainList;
		reader.imageUrlKeyChain = imageUrlChain;
		reader.urlKeyChain = urlChain;

		reader.relevantArticleQueue = relevantArticleQueue;
		reader.relevantArticleQueueLock = relevantArticleQueueLock;
		
		Thread t = new Thread(reader);
		threadMap.put(queueNum, t);
		runnableMap.put(queueNum, reader);
		
		t.start();	
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
		writer.filter = filter;
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
	 * printCurrentRunTime: given start time, prints current run time in an
	 * appropriate unit of measurement
	 *===============================================================================*/
	protected static long printCurrentRunTime(long startTime, String text) {
		DecimalFormat df = new DecimalFormat("#.00");
		long runTimeMillis = System.currentTimeMillis() - startTime;
		long runTime;
		String unit;
		if (runTimeMillis < MILLIS_PER_SEC) {
			runTime = runTimeMillis;
			unit = "mls";
		} else if (runTimeMillis < 2*MILLIS_PER_MIN) {
			runTime = runTimeMillis/MILLIS_PER_SEC;
			unit = "sec";
		} else if (runTimeMillis < 2*MILLIS_PER_HOUR) {
			runTime = runTimeMillis/MILLIS_PER_MIN;
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
	protected static long printCurrentRunTime(long startTime, String text, 
	String altThreadStamp) {
		DecimalFormat df = new DecimalFormat("#.00");
		long runTimeMillis = System.currentTimeMillis() - startTime;
		long runTime;
		String unit;
		if (runTimeMillis < MILLIS_PER_SEC) {
			runTime = runTimeMillis;
			unit = "mls";
		} else if (runTimeMillis < 2*MILLIS_PER_MIN) {
			runTime = runTimeMillis/MILLIS_PER_SEC;
			unit = "sec";
		} else if (runTimeMillis < 2*MILLIS_PER_HOUR) {
			runTime = runTimeMillis/MILLIS_PER_MIN;
			unit = "min";
		} else if (runTimeMillis < 3*MILLIS_PER_DAY) {
			runTime = runTimeMillis/MILLIS_PER_HOUR;
			unit = "hrs";
		} else {
			runTime = runTimeMillis/MILLIS_PER_SEC;
			unit = "days";
		}		
		String runTimeString = df.format(runTime);
		System.out.println(altThreadStamp + " " +  text + ": " + runTimeString + " " + unit);
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
	 * TESTING/DEBUGGING FUNCTIONS ***************************************************
	 *===============================================================================*/
	/*================================================================================
	 * clearAllData: deletes all Moreover data
	 *===============================================================================*/
	protected static boolean clearAllData() {
		
		clearBlobContainers();
		clearFromDatabase();
		clearQueues();
		
		printToConsole("waiting 30 seconds to attempt recreating blob containers...");
		try { Thread.sleep(30000); }
		catch (Exception e) {}
		
		createBlobContainers();

		return true;
	}
	/*================================================================================
	 * clearBlobContainers: deletes and recreates all blob containers
	 *===============================================================================*/
	protected static boolean clearBlobContainers() {
		MoreoverBlobOperator blobOp = new MoreoverBlobOperator();
		try {
			blobOp.connect(MoreoverBlobOperator.CONTENT_CONTAINER);
			blobOp.clearContainer(MoreoverBlobOperator.CONTENT_CONTAINER);
			printToConsole("content container deleted.");
			
			blobOp.connect(MoreoverBlobOperator.IMAGE_CONTAINER);
			blobOp.clearContainer(MoreoverBlobOperator.IMAGE_CONTAINER);
			printToConsole("image container deleted.");

			blobOp.connect(MoreoverBlobOperator.SOURCELOGO_CONTAINER);
			blobOp.clearContainer(MoreoverBlobOperator.SOURCELOGO_CONTAINER);
			printToConsole("source logo container deleted.");

			blobOp.connect(MoreoverBlobOperator.SUMMARY_CONTAINER);
			blobOp.clearContainer(MoreoverBlobOperator.SUMMARY_CONTAINER);
			printToConsole("summary container deleted.");
		} catch (Exception e) {
			printToConsole("EXCEPTION (" + e.getMessage() + ")");
		}
		return true;
	}
	/*================================================================================
	 * clearFromDatabase: clears all moreover information from database
	 *===============================================================================*/
	protected static boolean clearFromDatabase() {
		DBConnect con = new DBConnect();
		try {
			connectToDatabase(con);
			con.ExecuteStatement(document_attributes_statements.truncate());
			printToConsole("attributes truncated.");
			con.ExecuteStatement(document_relation_statements.truncate());
			printToConsole("relations truncated.");
			con.ExecuteStatement(document_statements.truncate());
			printToConsole("document truncated.");
		} catch (Exception e) {
			printToConsole("EXCEPTION (" + e.getMessage() + ")");
		}
		return true;
	}
	/*================================================================================
	 * clearQueues: clears all queues
	 *===============================================================================*/
	protected static boolean clearQueues() {
		MoreoverQueueOperator queueOp = new MoreoverQueueOperator();
		for (int i = 0; i < QUEUECOUNT; i++) {
			String queueName = QueueReaderRunnable.QUEUE_PREFIX + i;
			try {
				queueOp.connectQueue(queueName);
				queueOp.clearQueue(queueName);
				printToConsole(queueName + " cleared");
			} catch (Exception e) {
				printToConsole(queueName + ": EXCEPTION (" + e.getMessage() + ")");
			}
		}
		return true;
	}
	/*================================================================================
	 * createBlobContainers: creates all blob containers
	 *===============================================================================*/
	public static boolean createBlobContainers() {
		MoreoverBlobOperator blobOp = new MoreoverBlobOperator();
		try {
			blobOp.connect(MoreoverBlobOperator.CONTENT_CONTAINER);
			blobOp.connect(MoreoverBlobOperator.IMAGE_CONTAINER);
			blobOp.connect(MoreoverBlobOperator.SOURCELOGO_CONTAINER);
			blobOp.connect(MoreoverBlobOperator.SUMMARY_CONTAINER);
		} catch (Exception e) {
			printToConsole("EXCEPTION (" + e.getMessage() + ")");
		}
		return true;
	}
	/*================================================================================
	 * printQueuesizes: prints sizes of all queues
	 *===============================================================================*/
	protected static boolean printQueueSizes() {
		MoreoverQueueOperator queueOp = new MoreoverQueueOperator();
		for (int i = 0; i < QUEUECOUNT; i++) {
			String queueName = QueueReaderRunnable.QUEUE_PREFIX + i;
			try {
				queueOp.connectQueue(queueName);
				long queueSize = queueOp.getQueueLength(queueName);
				printToConsole(queueName + ": " + queueSize);
			} catch (Exception e) {
				printToConsole(queueName + ": EXCEPTION (" + e.getMessage() + ")");
			}
		}
		return true;
	}
	
}
