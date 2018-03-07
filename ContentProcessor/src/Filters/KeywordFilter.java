package Filters;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import dbconnect.general.document_relation_row;
import main.MoreoverArticle;

public class KeywordFilter extends Filter {

	public static List<String> keywordList;
	public static int matchThreshold = 3;
	
	@Override
	public void initialize(String threadStamp, boolean active) throws Exception {
		this.active = initializeSpecific(threadStamp, active);	
	}

	public void initialize(String threadStamp, int matchThreshold, boolean active) throws Exception {
		//TODO: if the file is empty, return zero size list. if this is the case, ignore this
		//part of the filtering process, and let all files through.
		
		this.active = active;
		if (!active) { return; }
		
		printToConsole(threadStamp, "readKeyWords function uninitialized. read 0 keywords");
		KeywordFilter.matchThreshold = matchThreshold;
		keywordList =  new Vector<String>();	
	}
	
	@Override
	public boolean relevanceCheck(MoreoverArticle article) throws Exception {
		
		if (!active) { return true; }
		
		int keyWordMatchCount = 0;
		for (String keyWord: keywordList) {
			if (keyWordMatchCount >= matchThreshold) { break; }
			if (doesMatch(article.title, keyWord) || doesMatch(article.content, keyWord)){
				keyWordMatchCount++;
				continue;
			}
		}
		if ((keyWordMatchCount < matchThreshold) && keywordList.size() > 0) {
			return false;
		}
		return true;
	}

	@Override
	public String populateArticle(MoreoverArticle article, Set<String> queriesFound) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void populateRelation(String query, document_relation_row relation) throws Exception {
		// TODO Auto-generated method stub
	}

}
