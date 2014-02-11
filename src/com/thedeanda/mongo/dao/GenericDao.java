package com.thedeanda.mongo.dao;

import java.util.ArrayList;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.thedeanda.mongo.MongoWrapper;

public abstract class GenericDao<T extends PersistedObject> {
	public static final Logger log = LoggerFactory.getLogger(GenericDao.class);

	protected MongoWrapper mongo;

	public abstract String getCollectionName();

	public abstract void initCollection();

	public abstract T deserialize(BasicDBObject o);

	public abstract BasicDBObject serialize(T t);

	public GenericDao(MongoWrapper mongoWrapprer) {
		this.mongo = mongoWrapprer;
	}

	public void init() throws Exception {
		if (!mongo.getDb().collectionExists(getCollectionName())) {
			mongo.getDb().createCollection(getCollectionName(), null);
		}
		initCollection();
	}

	public DBCollection getCollection() {
		return mongo.getDb().getCollection(getCollectionName());
	}

	public void delete(ObjectId id) {
		BasicDBObject json = new BasicDBObject();
		json.put("_id", id);
		getCollection().remove(json);
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
		BasicDBObject json = new BasicDBObject();
		json.put("_id", id);
		DBObject obj = getCollection().findOne(json);
		T ret = null;
		if (obj != null) {
			ret = deserialize((BasicDBObject) obj);
		}
		return ret;
	}

	/** returns a single matching item */
	public T getOne(BasicDBObject query) {
		DBObject obj = getCollection().findOne(query);
		if (obj != null) {
			return deserialize((BasicDBObject) obj);
		} else {
			return null;
		}
	}

	public int count(BasicDBObject query) {
		BasicDBObject json = null;
		if (query != null)
			json = query;
		else
			json = new BasicDBObject();

		DBCursor obj = getCollection().find(json);

		return obj.count();
	}

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

		DBCursor obj = getCollection().find(json);
		if (sort != null) {
			obj = obj.sort(sort);
		}
		if (offset > 0) {
			obj.skip(offset);
		}
		while (obj.hasNext() && count-- > 0) {
			DBObject item = obj.next();
			T u = deserialize((BasicDBObject) item);
			ret.add(u);
		}

		return ret;
	}

	public void remove(BasicDBObject qry) {
		getCollection().remove(qry);
	}

	public void save(T t) {
		BasicDBObject json = serialize(t);
		getCollection().save(json);
		t.setId(new ObjectId(json.getString("_id")));
	}

}
