package com.thedeanda.mongo;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

abstract public class MongoWrapper {
	private Mongo mongo;
	private DB db;

	public MongoWrapper(String host, int port, String db) throws UnknownHostException,
			MongoException {
		MongoOptions options = new MongoOptions();
		options.autoConnectRetry = true;
		options.connectTimeout = 5000;

		this.mongo = new Mongo(new ServerAddress(host, port), options);

		this.db = mongo.getDB(db);
		init();
	}

	abstract protected void init();

	protected DB getDb() {
		return db;
	};

}
