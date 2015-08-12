package com.thedeanda.mongo.dao;

import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;

public interface GenericDao<T extends PersistedObject> {

	public abstract String getCollectionName();

	public abstract void initCollection();

	public abstract T deserialize(Document o);

	public abstract Document serialize(T t);

	public abstract void init() throws Exception;

	public abstract MongoCollection<Document> getCollection();

	public abstract void delete(ObjectId id);

	public abstract void delete(BasicDBObject qry);

	public abstract T get(String id);

	public abstract T get(ObjectId id);

	/** returns a single matching item */
	public abstract T getOne(BasicDBObject query);

	public abstract long count(BasicDBObject query);

	/** returns the first 100 records */
	public abstract List<T> list();

	public abstract List<T> list(BasicDBObject query, BasicDBObject sort,
			int offset, int count);

	public abstract void save(T t);

}