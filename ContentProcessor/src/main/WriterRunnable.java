package main;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


import blob.moreover.MoreoverBlobOperator;

import dbconnect.general.moreover_article_row;
import dbconnect.general.moreover_article_statements;
import dbconnect.main.DBConnect;
import queue.moreover.MoreoverQueueOperator;
import xmlparser.AuxFileHandling.FieldChainInterpreter;
import xmlparser.Operations.XMLOperator;
import xmlparser.Types.FieldChain;
import xmlparser.Types.XMLNode;

public class WriterRunnable implements Runnable{

	public boolean terminate = false;
	public Exception exc = null;
	
	public static final String QUEUE_FINAL = "moreover-queue-final";
	public static final int READ_LIMIT = 500;
	public static final int SLEEP_TIME_MILLIS = 20000;
	
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
			"TEXT,location,country"
	)));
		
	public String writerThreadStamp = "(writer) ";
	
	/*================================================================================
	 * run
	 *===============================================================================*/
	@Override
	public void run() {
		
		System.out.println(writerThreadStamp + "running writer");
		
		try {
			
			//initialize blob and queue connections
			MoreoverBlobOperator blobOperator = new MoreoverBlobOperator();
			MoreoverQueueOperator queueOp = new MoreoverQueueOperator();
			blobOperator.connect(blobOperator.CONTENT_CONTAINER);
			queueOp.connectQueue(QUEUE_FINAL);
			
			while (!terminate) {
				
				//dequeue next message
				String nextArticle = null;
				try  { nextArticle = queueOp.dequeue(QUEUE_FINAL); }
				catch (NullPointerException ne) {
					Thread.sleep(SLEEP_TIME_MILLIS);
					continue;
				}
				
				//extract data and write to database
				moreover_article_row row = extractData(nextArticle);
				
				//write to blob
				String blobName = "article-"+row.sequenceId;
				blobOperator.writeBlob(blobOperator.CONTENT_CONTAINER, blobName, nextArticle); 
				
				//write article to database
				writeArticle(row);
				
				//delete message from queue
				queueOp.deleteLast(QUEUE_FINAL);
			}
			
		} catch (Exception e) { 
			exc = e;
			e.printStackTrace();
			terminate = true;
		}
		
		if (terminate) {
			System.out.println(writerThreadStamp + "thread terminated");
		}
		
	}	

	/*================================================================================
	 * extractData: given an xml article as a string, parses and returns desired content.
	 *===============================================================================*/
	protected moreover_article_row extractData(String contents) throws Exception {
		
		XMLOperator parser = new XMLOperator();
		moreover_article_row row = new moreover_article_row();
		
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
		
		return row;
	}

	/*================================================================================
	 * writeArticle: writes a list of articles to the database
	 *===============================================================================*/
	protected void writeArticle(moreover_article_row article) throws Exception {
		String statement = moreover_article_statements.InsertRow(article);
		DBConnect con = new DBConnect();
		con.ConnectProd();
		con.ExecuteStatement(statement);
		con.CommitClose();
	}

	/*================================================================================
	 * writeArticles: writes a list of articles to the database
	 *===============================================================================*/
	protected void writeArticles(List<moreover_article_row> articles) throws Exception {
		String statement = moreover_article_statements.InsertRows(articles);
		DBConnect con = new DBConnect();
		con.ConnectProd();
		con.ExecuteStatement(statement);
		con.CommitClose();
	}

	
	

}


