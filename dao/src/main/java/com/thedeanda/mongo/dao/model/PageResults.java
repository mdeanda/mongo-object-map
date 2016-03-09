package com.thedeanda.mongo.dao.model;

import java.io.Serializable;
import java.util.List;


public class PageResults<T extends PersistedObject> implements Serializable {
	private static final long serialVersionUID = 558322401682939903L;
	private long total;
	private long offset;
	private List<T> results;
	private long count;
	private boolean firstPage;
	private boolean lastPage;

	public PageResults(List<T> results, long offset, long count, long total,
			boolean firstPage, boolean lastPage) {
		this.results = results;
		this.offset = offset;
		this.count = count;
		this.total = total;
		this.firstPage = firstPage;
		this.lastPage = lastPage;
	}

	public long getTotal() {
		return total;
	}

	public long getOffset() {
		return offset;
	}

	public List<T> getResults() {
		return results;
	}

	public long getCount() {
		return count;
	}

	public boolean isFirstPage() {
		return firstPage;
	}

	public boolean isLastPage() {
		return lastPage;
	}
}
