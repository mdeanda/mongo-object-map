package com.thedeanda.mongo.dao;

import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.BasicDBObject;
import com.thedeanda.mongo.MongoWrapper;

public abstract class HazelcastCachedGenericDao<T extends PersistedObject> extends
		GenericDao<T> {

	private HazelcastInstance hazelcastInstance;

	public HazelcastCachedGenericDao(MongoWrapper mongoWrapprer,
			HazelcastInstance hazelcastInstance) {
		super(mongoWrapprer);

		this.hazelcastInstance = hazelcastInstance;
	}

	public abstract String getCacheSuffix();

	private String genMapKey(String part) {
		return "cache_dao_" + part + "_" + getCacheSuffix();
	}

	private void clearAllCaches() {
		String mapName = genMapKey("get");
		Map<String, T> cache = hazelcastInstance.getMap(mapName);
		cache.clear();

		mapName = genMapKey("list");
		Map<String, List<T>> listCache = hazelcastInstance.getMap(mapName);
		listCache.clear();

		mapName = genMapKey("count");
		Map<String, Integer> countCache = hazelcastInstance.getMap(mapName);
		countCache.clear();
	}

	private void removeFromCache(ObjectId id) {
		String mapName = genMapKey("get");
		Map<String, T> cache = hazelcastInstance.getMap(mapName);
		cache.remove(id.toString());

		mapName = genMapKey("list");
		Map<String, List<T>> listCache = hazelcastInstance.getMap(mapName);
		listCache.clear();

		mapName = genMapKey("count");
		Map<String, Integer> countCache = hazelcastInstance.getMap(mapName);
		countCache.clear();
	}

	public void delete(ObjectId id) {
		super.delete(id);
		removeFromCache(id);
	}

	public void remove(BasicDBObject qry) {
		super.remove(qry);
		clearAllCaches();
	}

	public T getCached(String id) {
		if (id == null)
			return null;

		String mapName = genMapKey("get");
		Map<String, T> cache = hazelcastInstance.getMap(mapName);
		T ret = cache.get(id);
		if (ret == null) {
			ret = get(id);
			if (ret != null)
				cache.put(id, ret);
		}
		return ret;
	}

	public T getCached(ObjectId id) {
		if (id != null)
			return getCached(id.toString());
		else
			return null;
	}

	public int countCached(BasicDBObject query) {
		String mapName = genMapKey("count");
		String key = query == null ? "" : query.toString();
		Map<String, Integer> cache = hazelcastInstance.getMap(mapName);
		Integer ret = cache.get(key);
		if (ret == null) {
			ret = count(query);
			cache.put(key, ret);
		}
		return ret;
	}

	public List<T> listCached(BasicDBObject query, BasicDBObject sort,
			int offset, int count) {
		String mapName = genMapKey("list");
		String key = (query == null ? "" : query.toString());
		key += "_" + (sort == null ? "" : sort.toString());
		key += String.format("_%d-%d", offset, count);
		Map<String, List<T>> cache = hazelcastInstance.getMap(mapName);
		List<T> ret = cache.get(key);
		if (ret == null) {
			ret = list(query, sort, offset, count);
			cache.put(key, ret);
		}
		return ret;
	}

	public void save(T t) {
		super.save(t);
		removeFromCache(t.getId());
	}
}
