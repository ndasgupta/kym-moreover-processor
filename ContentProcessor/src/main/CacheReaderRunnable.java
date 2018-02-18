package main;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import blob.moreover.MoreoverBlobOperator;
import dbconnect.general.document_attributes_row;
import dbconnect.general.document_attributes_statements;
import dbconnect.general.document_relation_row;
import dbconnect.general.document_relation_statements;
import dbconnect.general.document_row;
import dbconnect.general.document_statements;
import dbconnect.main.DBConnect;
import queue.moreover.MoreoverQueueOperator;
import xmlparser.AuxFileHandling.FieldChainInterpreter;
import xmlparser.Operations.XMLOperator;
import xmlparser.Types.FieldChain;
import xmlparser.Types.XMLNode;

/*TODO: should be able to dequeue many at a time. this is a change that must be made in
		AzureStorageConnect in the database workspace.
*/
public class CacheReaderRunnable implements Runnable {
	
	public int cacheNum = -1;
	public HashMap<String,Integer> genericNameMap = null;
	public HashMap<String,Integer> productMap = null;
	public HashMap<String,Integer> combinationIdMap = null;
	public List<String> categoryList = null;
	public List<String> editorialRankList = null;
	public boolean terminate = false;
	public Exception exc = null;
	
	public static final int CACHECOUNT = 20;
	
	public static final String QUEUE_PREFIX = "moreover-queue-";
	public static final String QUEUE_FINAL = "moreover-queue-final";
	public static final int READ_LIMIT = 500;
	public static final int SLEEP_TIME_MILLIS = 2000;
	public static final int REPORT_COUNT = 100;
	
	public static final String DOCUMENT_TYPE = "news";
	public static final String CONTENT_KEY = "content";
	public static final String SNIPPET_KEY = "summary";
	public static final String IMAGE_KEY = "image";
	public static final String RELEVANCY_KEY = "relevancy_scope";
	public static final String SOURCE_LOGO_KEY = "source_logo";
	public static final int SNIPPET_LENGTH = 1000;
	
	public static final String RELEVANCY_TITLE_VAL = "title";
	public static final String RELEVANCY_CONTENT_VAL = "content";
	
	public int exceptionCount = 0;
	
	public MoreoverBlobOperator blobOperator;
	private FieldChainInterpreter inter = new FieldChainInterpreter();	
	public List<FieldChain> chainList = inter.GetChainsDirect(new ArrayList<>(Arrays.asList(
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
		
	public String cacheThreadStamp;
	
	/*================================================================================
	 * run
	 *===============================================================================*/
	@Override
	public void run() {
		
		//check to make sure parameters were properly initialized
		if (cacheNum >= CACHECOUNT || cacheNum < 0) { 
			System.out.println("cacheNum not specified, "
					+ "or invalid cacheNum specified (" + cacheNum + ")");
			return;
		} 
		cacheThreadStamp = "(reader cache_" + cacheNum + ") ";
		if (genericNameMap == null || productMap == null || combinationIdMap == null) {
			System.out.println(cacheThreadStamp + "drug lists not initialized. value is null (" + cacheNum + ")");
			return;
		} else if (categoryList == null || editorialRankList == null) {
			System.out.println(cacheThreadStamp + "sources not initialized. value is null (" + cacheNum + ")");
			return;
		}
		
		System.out.println(cacheThreadStamp + "running cache reader");		
		
		try {			
			
			//intialize queue connections
			blobOperator = new MoreoverBlobOperator();
			blobOperator.connect(MoreoverBlobOperator.CONTENT_CONTAINER);
			blobOperator.connect(MoreoverBlobOperator.SUMMARY_CONTAINER);
			MoreoverQueueOperator queueOp = new MoreoverQueueOperator();
			String queueName = QUEUE_PREFIX + cacheNum;
			queueOp.connectQueue(queueName);
			queueOp.connectQueue(QUEUE_FINAL);
			
			//continuously dequeue and process messages until termination
			int repCount = 0;
			while (!terminate) {
				try {
					//dequeue next message
					
					String nextArticle = null;
					try  { nextArticle = queueOp.dequeue(queueName); }
					catch (NullPointerException ne) {
						Thread.sleep(SLEEP_TIME_MILLIS);
						continue;
					}
					repCount++;
					
					//extract data and check if relevant
					MoreoverArticle row = extractData(nextArticle);
					String drugFound = checkRelevantArticle(row);
					
					//if relevant, write to final queue
					if (drugFound != null) {				
						System.out.println(cacheThreadStamp + "RELEVANT ARTICLE FOUND: " 
								+ drugFound);
						writeToStorage(nextArticle, row);
					}
					
					//delete message from queue
					queueOp.deleteLast(queueName);
					
					if (repCount%100 == 0) {
						System.out.println(cacheThreadStamp + "processed " + repCount + " articles");
					}
				}
				catch (Exception e) {
					//Exceptions here:
					System.out.println(cacheThreadStamp + "exception: " + e.getMessage());
					e.printStackTrace();
					
					try { queueOp.deleteLast(queueName); }
					catch (Exception e1) { System.out.println("exception: failed to delete last queue item"); }
					
					exceptionCount+=1;
				}
			}
			
		}
		catch(Exception e) {
			exc = e;
			e.printStackTrace();
			terminate = true;
		}
		
		if (terminate) {
			System.out.println(cacheThreadStamp + "thread terminated");
		}
		
	}	
	/*================================================================================
	 * checkRelevantArticle: checks whether or not article is desired/relevant. If
	 * relevant, returns drug name that was found, otherwise returns null.
	 *===============================================================================*/
	protected String checkRelevantArticle(MoreoverArticle article) throws Exception {		
				
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
			return null;
		} else {
			System.out.println(article.category + "-" + article.editorialRank);
		}
		
		for (String drug: genericNameMap.keySet()) {
			if (doesMatch(article.title, drug) || doesMatch(article.content, drug)){
				return drug;
			}
		}	
		for (String drug: productMap.keySet()) {
			if (doesMatch(article.title, drug) || doesMatch(article.content, drug)){
				return drug;
			}
		}	
		
		return null;
	}
	
	/*================================================================================
	 * doesMatch
	 *===============================================================================*/
	public static boolean doesMatch(String content, String query) {
		
		if( query.replaceAll("[^a-zA-Z0-9]","").trim().length() <= 3 ){
			return false;
		}
		
		content = content.toLowerCase() ;
		content = content.length() >= 1000 ? content.substring(0, 999) : content ;
		content = content.replaceAll("[^a-zA-Z0-9]", "").trim() ;
		
		query = query.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim() ;
		
		if ( content.contains(query) )
			return true ;
		
		return false ;
	}

	/*================================================================================
	 * doesMatchFull
	 *===============================================================================*/
	public static boolean doesMatchFull(String content, String query) {
		
		if( query.replaceAll("[^a-zA-Z0-9]","").trim().length() <= 3 ){
			return false;
		}
		
		content = content.toLowerCase() ;
		content = content.replaceAll("[^a-zA-Z0-9]", "").trim() ;
		
		query = query.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim() ;
		
		if ( content.contains(query) )
			return true ;
		
		return false ;
	}
	/*================================================================================
	 * extractData: given an xml article as a string, parses and returns desired content.
	 *===============================================================================*/
	protected MoreoverArticle extractData(String contents) throws Exception {
		
		XMLOperator parser = new XMLOperator();
		MoreoverArticle row = new MoreoverArticle();
		
		HashMap<FieldChain, List<XMLNode>> nodeMap = parser.FieldChainParseString(contents,
				chainList); 

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
		
		return row;
	}
	
	/*================================================================================
	 * writeToStorage: writes article to the database and to blob storage.
	 *===============================================================================*/
	protected boolean writeToStorage(String xmlText, MoreoverArticle article) throws Exception {
		
		//TODO: write blob images
		
		//initialize document row
		document_row doc = new document_row();
		doc.type = DOCUMENT_TYPE;
		doc.url = article.url;
		doc.date = article.publishDate;
		doc.title = article.title;
		 
		//initialize attributes. Write each each attribute to blob storage, and intialize a
		//corresponding document attributes row.
		
		//article content
		String blobUrl = blobOperator.writeBlob(MoreoverBlobOperator.CONTENT_CONTAINER, 
				"article-"+article.sequenceId+".xml", xmlText);
		document_attributes_row docContent = new document_attributes_row();
		docContent.key = CONTENT_KEY;
		docContent.blob_url = blobUrl;
		
		//article summary/snippet
		String summaryBlobUrl = blobOperator.writeBlob(MoreoverBlobOperator.SUMMARY_CONTAINER, 
				"article-"+article.sequenceId+".xml", xmlText.substring(0,SNIPPET_LENGTH));
		document_attributes_row docSummary = new document_attributes_row();
		docSummary.key = SNIPPET_KEY;
		docSummary.blob_url = summaryBlobUrl;
		
		//initialize document relations. While checking for drugs in article/content, also
		//determined relevancy scope.
		document_attributes_row docRelevancy = new document_attributes_row();
		docRelevancy.key = RELEVANCY_KEY;
		docRelevancy.value = null;
		
		Set<String> namesFound = new HashSet<String>();
		
		for (String g: genericNameMap.keySet()) {
			if (doesMatch(article.title, g) || doesMatch(article.content, g)) {
				namesFound.add(g);
				if (docRelevancy.value == null) { docRelevancy.value = RELEVANCY_TITLE_VAL; }
			}
		}
		for (String p: productMap.keySet()) {
			if (doesMatch(article.title, p) || doesMatch(article.content, p)) {
				namesFound.add(p);
				if (docRelevancy.value == null) { docRelevancy.value = RELEVANCY_CONTENT_VAL; }
			}
		}
		List<document_relation_row> relations = new Vector<document_relation_row>();
		for (String s: namesFound) {
			document_relation_row docRel = new document_relation_row();
			docRel.generic_name_id = genericNameMap.get(s);
			docRel.product_id = productMap.get(s);
			docRel.combination_id = combinationIdMap.get(s);
			relations.add(docRel);
		}
		
		//connect and write document row, retreiving the id of the inserted row
		String docStatement = document_statements.InsertRow(doc);
		DBConnect con = new DBConnect();
		Main.connectToDatabase(con);
		int docId = con.ExecuteGetId(docStatement, "id");
		
		//update the other data rows with the returned id
		for (document_relation_row r: relations) {
			r.document_id = docId;
		}
		docContent.doc_id = docId;
		docSummary.doc_id = docId;
		docRelevancy.doc_id = docId;
		
		//construct statements and insert updated rows
		String docAttStatement = document_attributes_statements.InsertRows(
				new ArrayList<>(Arrays.asList(docContent,docSummary,docRelevancy)));
		String docRelationsStatement = document_relation_statements.InsertRows(relations);
		con.ExecuteStatement(docAttStatement);
		con.ExecuteStatement(docRelationsStatement);
		con.CommitClose();
		
		return true;
	}
	
	
}
