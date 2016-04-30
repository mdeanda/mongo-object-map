package com.thedeanda.mongo.dao;

import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.thedeanda.mongo.dao.model.PersistedObject;

public class HazelcastCachedGenericDao<T extends PersistedObject> implements
		GenericDao<T> {

	private HazelcastInstance hazelcastInstance;
	private GenericDao<T> dao;
	private String cacheKeyPrefix;

	private enum CACHE_SECTION {
		GET, LIST, COUNT
	}

	public HazelcastCachedGenericDao(GenericDao<T> dao,
			HazelcastInstance hazelcastInstance, String cacheKeyPrefix) {
		this.dao = dao;
		this.hazelcastInstance = hazelcastInstance;
		this.cacheKeyPrefix = cacheKeyPrefix;
	}

	private String genMapKey(CACHE_SECTION section) {
		return cacheKeyPrefix + "_cache_dao_" + section;
	}

	private void clearAllCaches() {
		for (CACHE_SECTION section : CACHE_SECTION.values()) {
			String mapName = genMapKey(section);
			hazelcastInstance.getMap(mapName).clear();
		}
	}

	@Override
	public String getCollectionName() {
		return dao.getCollectionName();
	}

	@Override
	public void initCollection() {
		dao.initCollection();
	}

	@Override
	public T deserialize(Document o) {
		return dao.deserialize(o);
	}

	@Override
	public Document serialize(T t) {
		return dao.serialize(t);
	}

	@Override
	public void init() throws Exception {
		dao.init();
	}

	@Override
	public MongoCollection<Document> getCollection() {
		return dao.getCollection();
	}

	@Override
	public void delete(ObjectId id) {
		dao.delete(id);
		clearAllCaches();
	}

	@Override
	public void delete(BasicDBObject qry) {
		dao.delete(qry);
		clearAllCaches();
	}

	@Override
	public T get(String id) {
		if (id == null)
			return null;

		String mapName = genMapKey(CACHE_SECTION.GET);
		IMap<String, T> cache = hazelcastInstance.getMap(mapName);
		T ret = cache.get(id);
		if (ret == null) {
			ret = dao.get(id);
			if (ret != null)
				cache.set(id, ret);
		}
		return ret;
	}

	@Override
	public T get(ObjectId id) {
		if (id == null)
			return null;

		String stringId = id.toString();
		String mapName = genMapKey(CACHE_SECTION.GET);
		IMap<String, T> cache = hazelcastInstance.getMap(mapName);
		T ret = cache.get(stringId);
		if (ret == null) {
			ret = dao.get(id);
			if (ret != null)
				cache.set(stringId, ret);
		}
		return ret;
	}

	@Override
	public T getOne(BasicDBObject query) {
		if (query == null)
			return null;

		String mapName = genMapKey(CACHE_SECTION.GET);
		String key = query == null ? "" : query.toString();
		IMap<String, T> cache = hazelcastInstance.getMap(mapName);
		T ret = cache.get(key);
		if (ret == null) {
			ret = dao.getOne(query);
			if (ret != null)
				cache.set(key, ret);
		}
		return ret;
	}

	@Override
	public long count(BasicDBObject query) {
		String mapName = genMapKey(CACHE_SECTION.COUNT);
		String key = query == null ? "" : query.toString();
		IMap<String, Long> cache = hazelcastInstance.getMap(mapName);
		Long ret = cache.get(key);
		if (ret == null) {
			ret = dao.count(query);
			cache.set(key, ret);
		}
		return ret;
	}

	@Override
	public List<T> list() {
		String mapName = genMapKey(CACHE_SECTION.LIST);
		String key = "";
		IMap<String, List<T>> cache = hazelcastInstance.getMap(mapName);
		List<T> ret = cache.get(key);
		if (ret == null) {
			ret = dao.list();
			cache.set(key, ret);
		}
		return ret;
	}

	@Override
	public List<T> list(BasicDBObject query, BasicDBObject sort, int offset,
			int count) {
		String mapName = genMapKey(CACHE_SECTION.LIST);
		String key = (query == null ? "" : query.toString());
		key += "_" + (sort == null ? "" : sort.toString());
		key += String.format("_%d-%d", offset, count);
		IMap<String, List<T>> cache = hazelcastInstance.getMap(mapName);
		List<T> ret = cache.get(key);
		if (ret == null) {
			ret = dao.list(query, sort, offset, count);
			cache.set(key, ret);
		}
		return ret;
	}

	@Override
	public void save(T t) {
		dao.save(t);
		clearAllCaches();
	}

}
