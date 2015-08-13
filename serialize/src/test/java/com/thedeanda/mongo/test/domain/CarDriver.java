package com.thedeanda.mongo.test.domain;

import com.thedeanda.mongo.StoredField;

public class CarDriver {
	@StoredField
	private String name;
	@StoredField
	private Car car;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Car getCar() {
		return car;
	}

	public void setCar(Car car) {
		this.car = car;
	}
}
