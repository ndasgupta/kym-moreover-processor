package main;


import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import blob.moreover.MoreoverBlobOperator;
import dbconnect.general.document_attributes_row;
import dbconnect.general.document_attributes_statements;
import dbconnect.general.document_relation_row;
import dbconnect.general.document_relation_statements;
import dbconnect.general.document_row;
import dbconnect.general.document_statements;

import dbconnect.main.DBConnect;

/*================================================================================
 * WriterRunnable
 * 
 * writes articles to the database and to blob storage. In each iteration of the 
 * loop, two blob storage writes and one database write take place. This is because:
 *  
 * 1. blobs cannot be written in batches, so no time would be saved by grouping them 
 * together.
 * 2. the id value of the document row is required as part of the other database 
 * attributes, and is only generated after insertion.
 * 
 * the other two inserts (for attriubutes & relations) are grouped together and
 * executed at the end. this turns what used to be two database writes per article
 * into two overall.
 *===============================================================================*/
public class WriterRunnable implements Runnable{

	//parameters
	List<MoreoverArticle> writeList = null;
	public HashMap<String,Integer> genericNameMap = null;
	public HashMap<String,Integer> productMap = null;
	public HashMap<String,Integer> combinationIdMap = null;	
	
	//return values
	public boolean threadCompleted = false;
	public boolean exceptionFound = false;
	
	//fixed parameters
	public String writerThreadStamp = "(reader writer) ";
	public static final int WRITE_LIMIT = 100;
	public static final int MAX_DB_WRITE = 1000;
	public static final int SNIPPET_LENGTH = 1000;
	public static final String DOCUMENT_TYPE = "news";
	public static final String CONTENT_KEY = "content";
	public static final String SNIPPET_KEY = "summary";
	public static final String IMAGE_KEY = "image";
	public static final String RELEVANCY_KEY = "relevancy_scope";
	public static final String SOURCE_LOGO_KEY = "source_logo";
		
	/*================================================================================
	 * run
	 *===============================================================================*/
	@Override
	public void run() {
		
		try {
			
			checkParameters();
			
			printToConsole("writer verified. writing " + writeList.size() + " articles...");
			
			//write articles
			List<document_attributes_row> attributesListFinal = new Vector<document_attributes_row>();
			List<document_relation_row> relationsListFinal = new Vector<document_relation_row>();
			MoreoverBlobOperator blobOperator = new MoreoverBlobOperator();
			DBConnect con = new DBConnect();
			connectAll(blobOperator, con);
			
			for (MoreoverArticle article: writeList) {
			
				//Write blob info first, so that generated urls can be used in attributes
				String blobUrl = blobOperator.writeBlob(MoreoverBlobOperator.CONTENT_CONTAINER, 
						"article-"+article.sequenceId+".xml", article.fullXml);
				String summaryBlobUrl = blobOperator.writeBlob(MoreoverBlobOperator.SUMMARY_CONTAINER, 
						"article-"+article.sequenceId+".xml", article.fullXml.substring(0,SNIPPET_LENGTH));
				
				//initialize database rows
				document_row doc = initDocumentRow(article);
				document_attributes_row docContent = initAttributeRow(CONTENT_KEY, blobUrl, null);
				document_attributes_row docSummary = initAttributeRow(SNIPPET_KEY, summaryBlobUrl, null);
				document_attributes_row docRelevancy = initAttributeRow(
						RELEVANCY_KEY, null, article.relevanceValue);
				List<document_relation_row> relations = initRelationsRows(article);
				
				//connect and write document row, retreiving the id of the inserted row
				String docStatement = document_statements.InsertRow(doc);
				int docId = con.ExecuteGetId(docStatement, "id");
				
				//update the other data rows with the returned id
				docContent.doc_id = docId;
				docSummary.doc_id = docId;
				docRelevancy.doc_id = docId;
				for (document_relation_row r: relations) {
					r.document_id = docId;
				}
				
				//add relations and attributes rows to final lists for end write.
				attributesListFinal.add(docContent);
				attributesListFinal.add(docSummary);
				attributesListFinal.add(docRelevancy);
				relationsListFinal.addAll(relations);
				
			}

			//execute final insert for all attribute and relation rows.
			writeAttributeListFinal(con, attributesListFinal);
			writeRelationListFinal(con, relationsListFinal);
			
			con.CommitClose();
			
			//declare completion.
			
		}
		catch(Exception e) {
			exceptionFound = true;
			printToConsole("exception: " + e.getMessage());
			e.printStackTrace();
		}
		
		threadCompleted = true;
		
	}	
	/*================================================================================
	 * checkParameters: checks that parameters have been properly initialized and throws
	 * exception if not.
	 *===============================================================================*/
	public void checkParameters() throws Exception {
		if (writeList == null) {
			throw new Exception("write list not initialized");
		}
		if (writeList.size() > WRITE_LIMIT) {
			throw new Exception("max write limit (" + WRITE_LIMIT + 
					") exceeded. list size: " + writeList.size());
		} else if (genericNameMap == null || productMap == null || combinationIdMap == null) {
			throw new Exception("drug lists not initialized. value is null");
		}
	}
	/*================================================================================
	 * connectAll: connects to cloud storage and database.
	 *===============================================================================*/
	public void connectAll(MoreoverBlobOperator blobOperator, DBConnect con) throws Exception {
		blobOperator.connect(MoreoverBlobOperator.CONTENT_CONTAINER);
		blobOperator.connect(MoreoverBlobOperator.SUMMARY_CONTAINER);
		Main.connectToDatabase(con);
	}
	/*================================================================================
	 * row initalizers: functions to initialize database row objects
	 *===============================================================================*/
	public document_row initDocumentRow(MoreoverArticle article) throws Exception {
		document_row row = new document_row();
		row.type = DOCUMENT_TYPE;
		row.url = article.url;
		row.date = article.publishDate;
		row.title = article.title;
		return row;
	}
	public document_attributes_row initAttributeRow(String key, String blobUrl, String value) 
	throws Exception {
		document_attributes_row row = new document_attributes_row();
		row.key = key;
		row.blob_url = blobUrl;
		row.value = value;
		return row;
	}
	public List<document_relation_row> initRelationsRows(MoreoverArticle article) 
	throws Exception {
		List<document_relation_row> relations = new Vector<document_relation_row>();
		for (String s: article.drugsFound) {
			document_relation_row docRel = new document_relation_row();
			docRel.generic_name_id = genericNameMap.get(s);
			docRel.product_id = productMap.get(s);
			docRel.combination_id = combinationIdMap.get(s);
			relations.add(docRel);
		}
		return relations;
	}

	/*================================================================================
	 * printToConsole: prints a string to the console, including thread identification
	 *===============================================================================*/
	protected void printToConsole(String statement) {
		System.out.println(writerThreadStamp + statement);
	}
	
	/*================================================================================
	 * writeFinalLists: writes final attribute and relation lists, accounting for the
	 * possibility that their size exceeds maximum.
	 *===============================================================================*/
	protected void writeAttributeListFinal(DBConnect con, List<document_attributes_row> attributesList) 
	throws Exception {
		
		//loop construct and execute attribute statements, in batches determined by the
		//MAX_DB_WRITE size.
		for (int i = 0; attributesList.size() > i*MAX_DB_WRITE; i++) {
			
			int toIndex = 0;
			if (attributesList.size() > (i+1)*MAX_DB_WRITE) {
				toIndex = (i+1)*MAX_DB_WRITE;
			} else {
				toIndex = attributesList.size();
			}
			
			String attStatement = document_attributes_statements.InsertRows(
					attributesList.subList(i*MAX_DB_WRITE, toIndex));			
			con.ExecuteStatement(attStatement);
		}
	}
	protected void writeRelationListFinal(DBConnect con, List<document_relation_row> relationsList)
	throws Exception {
		
		//identical loop for relation statements
		for (int i = 0; relationsList.size() > i*MAX_DB_WRITE; i++) {
			int toIndex = 0;
			if (relationsList.size() > (i+1)*MAX_DB_WRITE) {
				toIndex = (i+1)*MAX_DB_WRITE;
			} else {
				toIndex = relationsList.size();
			}
			String relStatement = document_relation_statements.InsertRows(
					relationsList.subList(i*MAX_DB_WRITE, toIndex));			
			con.ExecuteStatement(relStatement);
		}
	}

}


