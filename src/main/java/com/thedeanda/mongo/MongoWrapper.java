package com.thedeanda.mongo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

public class MongoWrapper {
	private static final Logger log = LoggerFactory
			.getLogger(MongoWrapper.class);

	private MongoClient mongo;
	private DB db;

	private String servers;
	private String dbName;

	public MongoWrapper() {
	}

	public void init() throws Exception {
		log.info("initialize db connection");
		log.info("dbname: " + dbName);

		List<ServerAddress> sas = new ArrayList<ServerAddress>();
		String[] serverList = StringUtils.split(servers);
		if (serverList != null) {
			for (String hostport : serverList) {
				String[] parts = hostport.split(":");
				String host = parts[0];
				int port = Integer.parseInt(parts[1]);
				log.info(String.format("server: %s:%d", host, port));
				sas.add(new ServerAddress(host, port));
			}
		}

		MongoClientOptions options = MongoClientOptions.builder()
				.connectTimeout(5000).socketTimeout(5000).build();

		if (!sas.isEmpty()) {
			this.mongo = new MongoClient(sas, options);
			mongo.setReadPreference(ReadPreference.secondaryPreferred());
			db = mongo.getDB(dbName);
		} else {
			log.warn("mongo not configured, no db connection available");
		}
	}

	public DB getDb() {
		return db;
	}

	public String getServers() {
		return servers;
	}

	public void setServers(String servers) {
		this.servers = servers;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public Mongo getMongo() {
		return mongo;
	};

}
