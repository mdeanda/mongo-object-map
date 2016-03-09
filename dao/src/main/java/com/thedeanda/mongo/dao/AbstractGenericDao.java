package com.thedeanda.mongo.dao;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.thedeanda.mongo.MongoWrapper;
import com.thedeanda.mongo.dao.model.PageResults;
import com.thedeanda.mongo.dao.model.PersistedObject;

public abstract class AbstractGenericDao<T extends PersistedObject> implements
		GenericDao<T> {
	public static final Logger log = LoggerFactory
			.getLogger(AbstractGenericDao.class);

	protected MongoClient mongo;

	private String database;

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#getCollectionName()
	 */
	@Override
	public abstract String getCollectionName();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#initCollection()
	 */
	@Override
	public abstract void initCollection();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#deserialize(org.bson.Document)
	 */
	@Override
	public abstract T deserialize(Document o);

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#serialize(T)
	 */
	@Override
	public abstract Document serialize(T t);

	public AbstractGenericDao(MongoClient mongo, String database) {
		this.mongo = mongo;
		this.database = database;
	}

	public AbstractGenericDao(MongoWrapper mongoWrapper) {
		this(mongoWrapper.getMongoClient(), mongoWrapper.getDbName());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#init()
	 */
	@Override
	public void init() throws Exception {
		/*
		 * if (!mongo.getDb().collectionExists(getCollectionName())) {
		 * mongo.getDb().createCollection(getCollectionName(), null); }
		 */
		initCollection();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#getCollection()
	 */
	@Override
	public MongoCollection<Document> getCollection() {
		return mongo.getDatabase(database).getCollection(getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#delete(org.bson.types.ObjectId)
	 */
	@Override
	public void delete(ObjectId id) {
		BasicDBObject json = new BasicDBObject();
		json.put("_id", id);
		getCollection().deleteOne(json);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thedeanda.mongo.dao.IGenericDao#delete(com.mongodb.BasicDBObject)
	 */
	@Override
	public void delete(BasicDBObject qry) {
		getCollection().deleteMany(qry);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#get(java.lang.String)
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#get(org.bson.types.ObjectId)
	 */
	@Override
	public T get(ObjectId id) {
		BasicDBObject query = new BasicDBObject();
		query.put("_id", id);
		return getOne(query);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thedeanda.mongo.dao.IGenericDao#getOne(com.mongodb.BasicDBObject)
	 */
	@Override
	public T getOne(BasicDBObject query) {
		Document obj = getCollection().find(query).first();
		T ret = null;
		if (obj != null) {
			ret = deserialize(obj);
		}
		return ret;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#count(com.mongodb.BasicDBObject)
	 */
	@Override
	public long count(BasicDBObject query) {
		BasicDBObject json = null;
		if (query != null)
			json = query;
		else
			json = new BasicDBObject();

		return getCollection().count(json);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#list()
	 */
	@Override
	public List<T> list() {
		return list(null, null, 0, 100);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#list(com.mongodb.BasicDBObject,
	 * com.mongodb.BasicDBObject, int, int)
	 */
	@Override
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#list(com.mongodb.BasicDBObject,
	 * com.mongodb.BasicDBObject, int, int)
	 */
	// @Override
	public PageResults<T> find(BasicDBObject query, BasicDBObject sort,
			int offset, int count) {
		List<T> ret = new ArrayList<>();

		BasicDBObject json = null;
		if (query != null)
			json = query;
		else
			json = new BasicDBObject();

		long total = count(query);

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

		count = ret.size();
		boolean last = offset + count >= total;
		boolean first = (offset <= 0);
		return new PageResults<T>(ret, offset, count, total, first, last);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.thedeanda.mongo.dao.IGenericDao#save(T)
	 */
	@Override
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
