package main;

import java.sql.Timestamp;

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
	
	public MoreoverArticle() { }
	
	public MoreoverArticle(int id, int sequenceId, String title, String content, String url, 
			int sourceId, String sourceName, String sourceUrl, String country, Timestamp publishDate,
			Timestamp recordDate, String category, String editorialRank) {
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
	}


}
