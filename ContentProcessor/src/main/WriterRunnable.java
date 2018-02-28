package main;


import java.awt.image.BufferedImage;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.imageio.ImageIO;

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
	public HashMap<String,Integer> conditionsMap = null;
	
	//return values
	public boolean threadCompleted = false;
	public boolean exceptionFound = false;
	
	//fixed parameters
	public String writerThreadStamp = "(reader writer) ";
	public static final int WRITE_LIMIT = 10;
	public static final int MAX_DB_WRITE = 1000;
	public static final int SNIPPET_LENGTH = 1000;
	public static final int MIN_NAME_LENGTH = 3;
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
			
			boolean writeImage;
			boolean writeSourceLogo;
			for (MoreoverArticle article: writeList) {
				
				//perform conditions search on the article.
				checkDrugNames(article);
				checkConditions(article);
				
				//attempt to write image blobs, and in doing so determine whether or not the
				//attributes are present.
				String imageBlobUrl = writeImageBlob(blobOperator, article.sequenceId, article.imageUrl);
				String sourceLogoBlobUrl = 
						writeImageBlob(blobOperator, article.sequenceId, article.sourceLogoUrl);
				if (imageBlobUrl == null) { writeImage = false; }
				else { writeImage = true; }
				if (sourceLogoBlobUrl == null) { writeSourceLogo = false; }
				else { writeSourceLogo = true; }
				
				//Write blob info first, so that generated urls can be used in attributes
				String blobUrl = blobOperator.writeBlobText(MoreoverBlobOperator.CONTENT_CONTAINER, 
						"article-"+article.sequenceId+".xml", article.fullXml);
				String summaryBlobUrl = blobOperator.writeBlobText(MoreoverBlobOperator.SUMMARY_CONTAINER, 
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
				
				//intialize the optional attributes and add to the list, if necessary
				if (writeImage) {
					document_attributes_row docImage = initAttributeRow(IMAGE_KEY, imageBlobUrl, 
							article.imageUrl);
					docImage.doc_id = docId;
					attributesListFinal.add(docImage);
				}
				if (writeSourceLogo) {
					document_attributes_row docSourceLogo = initAttributeRow(SOURCE_LOGO_KEY, 
							sourceLogoBlobUrl, article.sourceLogoUrl);
					docSourceLogo.doc_id = docId;
					attributesListFinal.add(docSourceLogo);
				}
				
				
			}

			//execute final insert for all attribute and relation rows.
			writeFinalAttributeList(con, attributesListFinal);
			writeFinalRelationList(con, relationsListFinal);
			
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
		} else if (genericNameMap == null || productMap == null || combinationIdMap == null
				|| conditionsMap == null) {
			throw new Exception("lists not initialized. value is null");
		}
	}
	/*================================================================================
	 * checkDrugNames: checks the title and first 1000 characters of the article for
	 * conditions.
	 *===============================================================================*/
	public void checkDrugNames(MoreoverArticle article) throws Exception {
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
		article.drugsFound = namesFound;
		article.relevanceValue = relevanceValue;
	}
	/*================================================================================
	 * checkConditions: checks the title and first 1000 characters of the article for
	 * conditions.
	 *===============================================================================*/
	public void checkConditions(MoreoverArticle article) throws Exception {
		Set<String> conditionsFound = new HashSet<String>();
		for (String c: conditionsMap.keySet()) {
			if (doesMatch(article.title, c) || doesMatch(article.content, c)) {
				conditionsFound.add(c);
			}
		}
		article.conditionsFound = conditionsFound;
	}
	/*================================================================================
	 * connectAll: connects to cloud storage and database.
	 *===============================================================================*/
	public void connectAll(MoreoverBlobOperator blobOperator, DBConnect con) throws Exception {
		blobOperator.connect(MoreoverBlobOperator.CONTENT_CONTAINER);
		blobOperator.connect(MoreoverBlobOperator.IMAGE_CONTAINER);
		blobOperator.connect(MoreoverBlobOperator.SOURCELOGO_CONTAINER);
		blobOperator.connect(MoreoverBlobOperator.SUMMARY_CONTAINER);
		Main.connectToDatabase(con);
	}
	/*================================================================================
	 * doesMatch: checks a body of text 'content' for appearance of a string 'query'
	 *===============================================================================*/
	public static boolean doesMatch(String content, String query) {
		
		//return if the query is too short.
		if( query.replaceAll("[^a-zA-Z0-9]","").trim().length() <= MIN_NAME_LENGTH){
			return false;
		}		
		content = content.length() >= SNIPPET_LENGTH ? content.substring(0, 
				SNIPPET_LENGTH-1) : content;
		content = content.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
		
		query = query.toLowerCase().replaceAll("[^a-zA-Z0-9]", "").trim();
		if (content.contains(query)) {
			return true ;
		}
		return false ;
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
		for (String s: article.conditionsFound) {
			document_relation_row docRel = new document_relation_row();
			docRel.condition_id = conditionsMap.get(s);
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
	 * writeImageBlob: writes an image blob from a given url, and returns the blob
	 * url.
	 *===============================================================================*/
	protected String writeImageBlob(MoreoverBlobOperator blobOperator, Long articleSeqId,
	String imageUrl) {
		
		if (imageUrl == null) { return null; }
		try {
			URL imgUrl = new URL(imageUrl);
			BufferedImage img = ImageIO.read(imgUrl);
			return blobOperator.writeBlobImage(MoreoverBlobOperator.IMAGE_CONTAINER, 
					"image-"+articleSeqId+".jpg", img);
		} catch(Exception e) {
			printToConsole("image blob exception: " + e.getMessage());
			return null;
		}
	}
	/*================================================================================
	 * writeFinalLists: writes final attribute and relation lists, accounting for the
	 * possibility that their size exceeds maximum.
	 *===============================================================================*/
	protected void writeFinalAttributeList(DBConnect con, List<document_attributes_row> attributesList) 
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
	protected void writeFinalRelationList(DBConnect con, List<document_relation_row> relationsList)
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


