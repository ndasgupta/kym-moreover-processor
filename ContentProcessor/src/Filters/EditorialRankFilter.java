package Filters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import dbconnect.general.document_relation_row;
import main.MoreoverArticle;

public class EditorialRankFilter extends Filter {

	public final List<String> editorialRankList = new ArrayList<>(Arrays.asList(
			"1","2","3","4"));
	
	@Override
	public void initialize(String threadStamp, boolean active) throws Exception {
		this.active = active;
	}

	@Override
	public boolean relevanceCheck(MoreoverArticle article) throws Exception {
		
		if (!active) { return false; }
		
		for (String r: editorialRankList) {
			if (r.trim().toLowerCase().equals(article.editorialRank.trim().toLowerCase())) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String populateArticle(MoreoverArticle article, Set<String> queriesFound) throws Exception {
		return null;
	}

	@Override
	public void populateRelation(String query, document_relation_row relation) throws Exception { }

}
