package main;

import java.sql.Timestamp;
import java.util.Set;

public class MoreoverArticle {
	
	public Integer id;
	public long sequenceId;
	public String title;
    public String content;
    public String url;
    public int sourceId;
    public String sourceName;
    public String sourceUrl;
    public String country;
    public Timestamp publishDate;
    public Timestamp recordDate;
    public String category;
    public String editorialRank;
    public String fullXml;
    
    public Set<String> drugsFound;
    public String relevanceValue;
    public String firstDrugFound;
    
    protected boolean isRelevant;
    
    public static final String RELEVANCY_TITLE_VAL = "title";
	public static final String RELEVANCY_CONTENT_VAL = "content";
	
	public MoreoverArticle() { }
	
	public MoreoverArticle(MoreoverArticle ref) { 
		this.id = ref.id;
		this.sequenceId = ref.sequenceId;
		this.title = ref.title;
		this.content = ref.content;
		this.url = ref.url;
		this.sourceId = ref.sourceId;
		this.sourceName = ref.sourceName;
		this.sourceUrl = ref.sourceUrl;
		this.country = ref.country;
		this.publishDate = ref.publishDate;
		this.recordDate = ref.recordDate;
		this.category = ref.category;
		this.editorialRank = ref.editorialRank;
		this.fullXml = ref.fullXml;
		
		drugsFound = null;
	    relevanceValue = null;
	    firstDrugFound = null;
	    isRelevant = false;
	}
	
	public MoreoverArticle(int id, int sequenceId, String title, String content, String url, 
			int sourceId, String sourceName, String sourceUrl, String country, Timestamp publishDate,
			Timestamp recordDate, String category, String editorialRank, String fullXml) {
		this.id = id;
		this.sequenceId = sequenceId;
		this.title = title;
		this.content = content;
		this.url = url;
		this.sourceId = sourceId;
		this.sourceName = sourceName;
		this.sourceUrl = sourceUrl;
		this.country = country;
		this.publishDate = publishDate;
		this.recordDate = recordDate;
		this.category = category;
		this.editorialRank = editorialRank;
		this.fullXml = fullXml;
		
		drugsFound = null;
	    relevanceValue = null;
	    firstDrugFound = null;
	    isRelevant = false;
	}
	/*================================================================================
	 * declareRelevant: denotes this article as relevant, and initializes all necessary
	 * variables for a relevant article.
	 *===============================================================================*/
	public void declareRelevant(Set<String> drugsFound, String relevanceValue, String firstDrugFound)
	throws Exception{
		if (!relevanceValue.equals(RELEVANCY_TITLE_VAL) && 
				!relevanceValue.equals(RELEVANCY_CONTENT_VAL)) {
			throw new Exception("must enter predetermined static value for relevanceValue");
		}
		
		this.drugsFound = drugsFound;
		this.relevanceValue = relevanceValue;
		this.firstDrugFound = firstDrugFound;
		
		isRelevant = true;
	}
	
	public boolean relevant() {
		return isRelevant;
	}

}
