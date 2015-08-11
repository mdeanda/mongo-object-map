package com.thedeanda.mongo.test.domain;

import org.bson.types.ObjectId;

import com.thedeanda.mongo.StoredField;
import com.thedeanda.mongo.StoredIdField;

public class Car {
	@StoredIdField
	private ObjectId id;
	@StoredField
	private String model;
	@StoredField
	private String type;
	@StoredField
	private int price;

	public ObjectId getId() {
		return id;
	}

	public String getModel() {
		return model;
	}

	public String getType() {
		return type;
	}

	public int getPrice() {
		return price;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public void setModel(String model) {
		this.model = model;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setPrice(int price) {
		this.price = price;
	}
}
