package Filters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import dbconnect.general.document_relation_row;
import main.MoreoverArticle;

public class CategoryFilter extends Filter {

	public static final List<String> categoryList = new  ArrayList<>(Arrays.asList(
			"academic",
			"general",
			"government",
			"journal",
			"local",
			"national",
			"organization"));
	
	@Override
	public void initialize(String threadStamp, boolean active) throws Exception {
		this.active = active;
	}

	@Override
	public boolean relevanceCheck(MoreoverArticle article) throws Exception {
		
		if (!active) { return false; }
		
		for (String c: categoryList) {
			if (c.trim().toLowerCase().equals(article.category.trim().toLowerCase())) {
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
