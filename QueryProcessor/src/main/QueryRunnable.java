package main;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.xml.XmlPage;

import queue.moreover.MoreoverQueueOperator;
import xmlparser.Operations.XMLOperator;
import xmlparser.Types.FieldChain;
import xmlparser.Types.XMLNode;


/*
 * notes:
 * one queue assigned to each runnable as a parameter.
 * */


public class QueryRunnable implements Runnable {

	//parameters
	public int queueNum = -1;
	public String seqId = null;
	public FieldChain articleChain = null;
	public FieldChain seqIdChain = null;
	public WebClient chrome = null;
	
	//return values
	public boolean threadCompleted = false;
	public boolean seqIdFound = false;
	public boolean exceptionFound = false;
	public String nextSeqId = "";
	
	//fixed parameters
	public static final int QUEUECOUNT = 20;	
	public static final String ACCESS_KEY = "da19e4764d5746eca821adda51031e4c";
	public static final String URL_START = "http://metabase.moreover.com/api/v10/articles?key=";
	public static final String URL_SEQUENCE_ID = "&sequence_id=";
	public static final String XML_HEADER = "<?xml version=\"1.0\"?>\n";
	public static final String queuePrefix = "moreover-queue-";	
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSS");
	public String threadStamp;
	
	//general time parameters
	public static final int MILLIS_PER_SEC = 1000;
	
	/*================================================================================
	 * main
	 *===============================================================================*/
	@Override
	public void run() {
		
		long startTime = System.currentTimeMillis();
		
		try {
			
			//check that all thread parameters have been correctly initialized
			if (queueNum >= QUEUECOUNT || queueNum < 0) { 
				throw new Exception("cacheNum invalid, or not specified  (" + queueNum + ")");
			}
			threadStamp = "(query queue" + queueNum + ") ";;
			if (articleChain == null || seqIdChain == null || seqId == null || chrome == null) {
				throw new Exception("not all parameters were properly initialized");
			}		
						
			//construct sequence id component of request url
			String sequenceIdUrl = "";
			if (seqId != "") { sequenceIdUrl = URL_SEQUENCE_ID + seqId; }

			//make api call through http request and store resulting xml page as string
			printToConsole("query validated. requesting data...");
			
			XmlPage page = chrome.getPage(URL_START + ACCESS_KEY + sequenceIdUrl);
			String xmlString = page.asXml().toString();
							
			//iterate through the string, identify and isolate article elements
			List<String> articleList = parseForArticles(xmlString);		
			printToConsole("articles found: " + articleList.size());
			
			//get max sequence id so it can be used by the next queryer
			nextSeqId = Long.toString(getMaxSeqId(articleList));
			if (nextSeqId == "" || nextSeqId == null) {
				throw new Exception("sequence id error");
			}
			seqIdFound = true;
			
			//write articles' xml content to azure queue
			printToConsole("writing files...");
			writeToQueue(articleList);
											
		} catch (Exception e) {
			exceptionFound = true;
			printToConsole("exception: " + e.getMessage());
			e.printStackTrace();
		}
		
		printToConsole("terminating thread (" + getCurrentRunTimeSec(startTime) + " sec)");
		threadCompleted = true;
		
	}
	
	/*================================================================================
	 * COMPLEX FUNCTIONS
	 *===============================================================================*/
	/*===============================================================================*/
	/*================================================================================
	 * getMaxSeqId: given a list of article strings, identifies the maximum sequence id
	 *===============================================================================*/
	protected long getMaxSeqId(List<String> articleList) throws Exception {
		
		long maxSeqId = 0;
		XMLOperator parser = new XMLOperator();
		
		for (String a: articleList) {
			
			//identify the sequence id
			HashMap<FieldChain, List<XMLNode>> seqIdMap = parser.FieldChainParseString(a,
					new ArrayList<>(Arrays.asList(seqIdChain)));
			if (seqIdMap.get(seqIdChain).size() > 1) {
				throw new Exception("parsing error. multipe sequence ids found for single article");
			}
			long tempId = Long.parseLong(seqIdMap.get(seqIdChain).get(0).innerXml.trim());
			
			//keep track of max sequence id
			if (tempId > maxSeqId) { maxSeqId = tempId; }
		}
		
		return maxSeqId;
	}
	/*================================================================================
	 * parseForArticles: parses the full xml response and returns a list of article
	 * nodes as strings.
	 *===============================================================================*/
	protected List<String> parseForArticles(String fullXml) throws Exception {
		
		List<String> articleList = new Vector<String>();
		XMLOperator op = new XMLOperator();
		
		HashMap<FieldChain, List<XMLNode>> nodeMap = op.FieldChainParseString(cleanXml(fullXml),
				new ArrayList<>(Arrays.asList(articleChain)));
		List<XMLNode> articleNodes = nodeMap.get(articleChain);
				
		for (XMLNode n: articleNodes) {
			articleList.add(XML_HEADER + "<article>" + n.innerXml + "</article>");
		}			
		
		return articleList;
	}
	
	/*================================================================================
	 * writeToQueue: writes list of articles to the filesystem temp cache, and
	 * returns the maximum sequence id
	 *===============================================================================*/
	protected void writeToQueue(List<String> articleList) throws Exception {
		
		String queueName = queuePrefix + queueNum;
		XMLOperator parser = new XMLOperator();
		
		for (String a: articleList) {
			//identify the sequence id
			HashMap<FieldChain, List<XMLNode>> seqIdMap = parser.FieldChainParseString(a,
					new ArrayList<>(Arrays.asList(seqIdChain)));
			if (seqIdMap.get(seqIdChain).size() > 1) {
				throw new Exception("parsing error. multipe sequence ids found for single article");
			}
		}
		
		MoreoverQueueOperator queueOp = new MoreoverQueueOperator();
		queueOp.connectQueue(queueName);
		queueOp.enqueue(queueName, articleList);

	}
	/*================================================================================
	 * SIMPLE FUNCTIONS
	 *===============================================================================*/
	/*===============================================================================*/
	/*================================================================================
	 * cleanXml: clean up xml string for safe SAX parsing.
	 *===============================================================================*/
	protected String cleanXml(String rawXml) {
		return rawXml.replaceAll("&", "&amp;");
	}
	/*================================================================================
	 * getCurrentRunTime: given start time, returns current run time in seconds
	 *===============================================================================*/
	protected long getCurrentRunTimeSec(long startTime) {
		return (System.currentTimeMillis() - startTime)/MILLIS_PER_SEC;
	}
	/*================================================================================
	 * printToConsole: prints a string to the console, including thread identification
	 *===============================================================================*/
	protected void printToConsole(String statement) {
		System.out.println(threadStamp + statement);
	}

}
