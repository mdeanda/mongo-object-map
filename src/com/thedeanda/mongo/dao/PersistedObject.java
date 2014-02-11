package com.thedeanda.mongo.dao;

import java.io.Serializable;

import org.bson.types.ObjectId;

public interface PersistedObject extends Serializable {
	public ObjectId getId();
	public void setId(ObjectId id);
}
