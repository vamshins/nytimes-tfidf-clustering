package com.unm.app.nyt.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NewsItemReply {
	
	@JsonProperty(value="status")
	private String status;
	
	@JsonProperty(value="copyright")
	private String copyRight;
	
	@JsonProperty(value="num_results")
	private Long numberOfResults;
	
	@JsonProperty(value="results")
	private List<NewsItem> results;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getCopyRight() {
		return copyRight;
	}

	public void setCopyRight(String copyRight) {
		this.copyRight = copyRight;
	}

	public Long getNumberOfResults() {
		return numberOfResults;
	}

	public void setNumberOfResults(Long numberOfResults) {
		this.numberOfResults = numberOfResults;
	}

	public List<NewsItem> getResults() {
		return results;
	}

	public void setResults(List<NewsItem> results) {
		this.results = results;
	}

}
