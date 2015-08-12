package com.thedeanda.mongo.dao;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public abstract class GenericDao<T extends PersistedObject> {
	public static final Logger log = LoggerFactory.getLogger(GenericDao.class);

	protected MongoClient mongo;

	private String database;

	public abstract String getCollectionName();

	public abstract void initCollection();

	public abstract T deserialize(Document o);

	public abstract Document serialize(T t);

	public GenericDao(MongoClient mongo, String database) {
		this.mongo = mongo;
		this.database = database;
	}

	public void init() throws Exception {
		/*
		 * if (!mongo.getDb().collectionExists(getCollectionName())) {
		 * mongo.getDb().createCollection(getCollectionName(), null); }
		 */
		initCollection();
	}

	public MongoCollection<Document> getCollection() {
		return mongo.getDatabase(database).getCollection(getCollectionName());
	}

	public void delete(ObjectId id) {
		BasicDBObject json = new BasicDBObject();
		json.put("_id", id);
		getCollection().deleteOne(json);
	}

	public void delete(BasicDBObject qry) {
		getCollection().deleteMany(qry);
	}

	public T get(String id) {
		ObjectId oid = null;
		try {
			oid = new ObjectId(id);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			oid = null;
		}
		if (oid != null)
			return get(oid);
		else
			return null;
	}

	public T get(ObjectId id) {
		BasicDBObject query = new BasicDBObject();
		query.put("_id", id);
		return getOne(query);
	}

	/** returns a single matching item */
	public T getOne(BasicDBObject query) {
		Document obj = getCollection().find(query).first();
		T ret = null;
		if (obj != null) {
			ret = deserialize(obj);
		}
		return ret;
	}

	public long count(BasicDBObject query) {
		BasicDBObject json = null;
		if (query != null)
			json = query;
		else
			json = new BasicDBObject();

		return getCollection().count(json);
	}

	/** returns the first 100 records */
	public List<T> list() {
		return list(null, null, 0, 100);
	}

	public List<T> list(BasicDBObject query, BasicDBObject sort, int offset,
			int count) {
		List<T> ret = new ArrayList<T>();

		BasicDBObject json = null;
		if (query != null)
			json = query;
		else
			json = new BasicDBObject();

		FindIterable<Document> obj = getCollection().find(json);
		if (sort != null) {
			obj = obj.sort(sort);
		}
		if (offset > 0) {
			obj.skip(offset);
		}
		obj.batchSize(count);
		for (Document doc : obj) {
			T u = deserialize(doc);
			ret.add(u);
			count--;
			if (count <= 0)
				break;
		}

		return ret;
	}

	public void save(T t) {
		Document json = serialize(t);
		if (t.getId() == null) {
			getCollection().insertOne(json);
			t.setId(new ObjectId(json.getString("_id")));
		} else {
			Bson filter = new BasicDBObject("_id", t.getId());
			getCollection().replaceOne(filter, json);
		}
	}

}
