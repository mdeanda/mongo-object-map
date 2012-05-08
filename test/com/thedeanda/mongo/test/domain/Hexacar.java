package com.thedeanda.mongo.test.domain;

import com.thedeanda.mongo.StoredField;

public class Hexacar extends Car {
	@StoredField
	private String shape;

	public String getShape() {
		return shape;
	}

	public void setShape(String shape) {
		this.shape = shape;
	}
}
