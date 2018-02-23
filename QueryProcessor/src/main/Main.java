package main;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.logging.Level;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;

import queue.moreover.MoreoverQueueOperator;
import xmlparser.AuxFileHandling.FieldChainInterpreter;
import xmlparser.Types.FieldChain;


public class Main {
	
	//querying parameters
	public static final int CALL_FREQUENCY_SEC = 30;
	public static final int CALL_FREQUENCY_MILLIS = CALL_FREQUENCY_SEC*1000;
	public static final int SHORT_PAUSE_MILLIS = 100;
	public static final int QUEUECOUNT = 20;
	
	//general time parameters
	public static final int MILLIS_PER_SEC = 1000;
	public static final int MILLIS_PER_MIN = 60*MILLIS_PER_SEC;
	public static final int MILLIS_PER_HOUR = 60*MILLIS_PER_MIN;
	public static final int MILLIS_PER_DAY = 24*MILLIS_PER_HOUR;
		
	//parsing variables
	public static final String articleChainSpecs = "TEXT,articles,article";
	public static final String seqIdSpecs = "TEXT,sequenceId";
	public static FieldChain articleChainMaster;
	public static FieldChain seqIdChainMaster;
	
	//print statement identification.
	public static final String mainThreadStamp = "(querier main) ";
			

	/*================================================================================
	 * main
	 *===============================================================================*/
	public static void main(String[] args) {
	
		//initialize browsing and parsing variables
		FieldChainInterpreter inter = new FieldChainInterpreter();
		articleChainMaster = inter.GetChainsDirect(articleChainSpecs);
		seqIdChainMaster = inter.GetChainsDirect(seqIdSpecs);
		WebClient chromeClient = getChromeClient();
		
		//map so that all runnables and threads can be accurately referenced
		HashMap<Integer,QueryRunnable> runnableMap = new HashMap<Integer,QueryRunnable>();		
		
		//variables to track status of querier, and transition between iterations
		long globalStartTime = System.currentTimeMillis();			
		int exceptionCount = 0;
		long lastQueryStart = System.currentTimeMillis() - (CALL_FREQUENCY_MILLIS+1);
		String currSeqId = "";
		
		//intialize placeholder values into runnable map, to avoid exception on first iteration
		for (int i = 0; i < QUEUECOUNT; i++) {
			QueryRunnable placeHolderRunnable = new QueryRunnable();
			placeHolderRunnable.threadCompleted = true;
			runnableMap.put(i, placeHolderRunnable);
		}

		for (long i = 0; true; i++) {
			
			try {
				//check if last thread of this num is finished running. wait if not.
				while (!runnableMap.get((int)(i%QUEUECOUNT)).threadCompleted) {
					Thread.sleep(SHORT_PAUSE_MILLIS);
				}
	
				//wait until requisite call frequency wait is reached
				if (getCurrentRunTimeMillis(lastQueryStart) < CALL_FREQUENCY_MILLIS) {
					long waitTime = CALL_FREQUENCY_MILLIS - getCurrentRunTimeMillis(lastQueryStart);
					if (waitTime > 0) {
						Thread.sleep(waitTime);
					}
				} else { printToConsole("call frequency wait time exceeded."); }
				
				//intialize and run query, recording start time
				QueryRunnable querier = executeQueryRunnable((int)(i%QUEUECOUNT), currSeqId, chromeClient);
				lastQueryStart = System.currentTimeMillis();
				
				//put in map for later access
				runnableMap.put((int)(i%QUEUECOUNT), querier);
				
				//wait for next sequence id
				while (!querier.seqIdFound && !querier.exceptionFound) {
					Thread.sleep(SHORT_PAUSE_MILLIS);
				} 
				
				//after a few seconds, querier must either have determined the next sequence
				//id, or terminated because of an excepiton. respond to both possibilities.
				if (querier.seqIdFound) {
					currSeqId = querier.nextSeqId;
				} else if (querier.exceptionFound) {
					exceptionCount+=1;
				}
				
				//print statements to keep track of operation.
				printToConsole("iteration complete. count: " + i);
				if (i%QUEUECOUNT == 0) {
					printToConsole("exception count: " + exceptionCount);
					printCurrentRunTimeDays(globalStartTime, "current run time: ");
				}
				
			} catch(Exception e) {				
				printToConsole("EXCEPTION: " + e.getMessage() + ". iteration count = " + i);	
			}
			
		}		
	}
	

	/*================================================================================
	 * SIMPLE FUNCTIONS **************************************************************
	 *===============================================================================*/
	/*================================================================================
	 * getCurrentRunTime: given start time, returns current run time in seconds
	 *===============================================================================*/
	protected static long getCurrentRunTimeMillis(long startTime) throws Exception {
		return (System.currentTimeMillis() - startTime);
	}
	protected static long getCurrentRunTimeSec(long startTime) throws Exception {
		return (System.currentTimeMillis() - startTime)/MILLIS_PER_SEC;
	}
	/*================================================================================
	 * getChromeClient: intializes webclient, ignoring built in javascript errors loggings
	 *===============================================================================*/
	protected static WebClient getChromeClient() {
		java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(Level.OFF);		
		printToConsole("initializing WebClient (BrowserVersion.CHROME)...");
		return new WebClient(BrowserVersion.CHROME);
	}
	/*================================================================================
	 * printCurrentRunTime: given start time, prints current run time in the specified
	 * unit of measurement.
	 *===============================================================================*/
	protected static long printCurrentRunTimeSec(long startTime, String text) throws Exception {
		DecimalFormat df = new DecimalFormat("#.00");
		long runTime = (System.currentTimeMillis() - startTime)/MILLIS_PER_SEC;		
		String runTimeString = df.format(runTime);
		printToConsole(text + ": " + runTimeString + " sec");
		return runTime;
	}
	protected static long printCurrentRunTimeHours(long startTime, String text) throws Exception {
		DecimalFormat df = new DecimalFormat("#.00");
		long runTime = (System.currentTimeMillis() - startTime)/MILLIS_PER_HOUR;		
		String runTimeString = df.format(runTime);
		printToConsole(text + ": " + runTimeString + " hours");
		return runTime;
	}
	protected static long printCurrentRunTimeDays(long startTime, String text) throws Exception {
		DecimalFormat df = new DecimalFormat("#.000");
		long runTime = (System.currentTimeMillis() - startTime)/MILLIS_PER_DAY;		
		String runTimeString = df.format(runTime);
		printToConsole(text + ": " + runTimeString + " days");
		return runTime;
	}
	/*================================================================================
	 * printToConsole: prints a string to the console, including thread identification
	 *===============================================================================*/
	protected static void printToConsole(String statement) {
		System.out.println(mainThreadStamp + statement);
	}
	/*================================================================================
	 * executeQueryRunnable: intialize QueryRunnable parameter, and runs as a thread
	 *===============================================================================*/
	protected static QueryRunnable executeQueryRunnable(int queueNum, String seqId, 
	WebClient client) throws Exception {
		QueryRunnable qr = new QueryRunnable();
		qr.queueNum = queueNum;
		qr.seqId = seqId;
		qr.articleChain = articleChainMaster;
		qr.seqIdChain = seqIdChainMaster;
		qr.chrome = client;
		Thread t = new Thread(qr);
		t.start();
		return qr;
	}
	
	/*================================================================================
	 * TESTING/DEBUGGING FUNCTIONS ***************************************************
	 *===============================================================================*/
	/*================================================================================
	 * printQueuesizes: prints sizes of all queues
	 *===============================================================================*/
	protected static void printQueueSizes() throws Exception {
		String queuePrefix = "moreover-queue-";
		MoreoverQueueOperator queueOp = new MoreoverQueueOperator();
		for (int i = 0; i < QUEUECOUNT; i++) {
			String queueName = queuePrefix + i;
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
