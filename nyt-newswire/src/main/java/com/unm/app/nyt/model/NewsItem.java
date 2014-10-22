package com.unm.app.nyt.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class NewsItem {

	@JsonProperty(value = "abstract")
	private String abstractForNews;

	@JsonProperty(value="url")
	private String url;

	public String getAbstractForNews() {
		return abstractForNews;
	}

	public void setAbstractForNews(String abstractForNews) {
		this.abstractForNews = abstractForNews;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

}
