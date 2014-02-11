package com.thedeanda.mongo.dao;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;

/**
 * migration script that looks at files named numerically starting from 1.json
 * in /resources of the classpath
 * 
 * @author mdeanda
 * 
 */
public class DbMigration {
	private static final String VERSION_COL = "dbversion";
	private static final ObjectId VERSION_ID = new ObjectId(0, 0, 0);
	private static final String VERSION_FIELD = "version";
	private static final String RESOURCE_BASE = "/dbmigration/";
	private static final String RESOURCE_EXTENSION = ".js";

	private static final Logger log = LoggerFactory
			.getLogger(DbMigration.class);
	private DB db;

	public DbMigration(DB db) {
		this.db = db;
	}

	public void run() throws Exception {
		int version = getVersion();
		log.warn(String.format("db version is: %s", version));

		boolean working = true;
		while (working) {
			int nextVersion = version + 1;
			String json = loadVersionFile(nextVersion);
			if (!StringUtils.isBlank(json)) {
				log.warn("running db migrate script for version: "
						+ nextVersion);
				log.warn(json);
				CommandResult result = db.doEval(json);
				if (result.ok()) {
					log.info(result.toString());
					version = nextVersion;
				} else {
					log.error(result.getErrorMessage());
					log.error(result.toString());
					working = false;
					result.throwOnError();
				}
			} else {
				working = false;
			}

			if (working)
				saveVersion(version);
		}
	}

	private void saveVersion(int version) {
		log.warn("save db migration version: " + version);
		BasicDBObject obj = new BasicDBObject("_id", VERSION_ID);
		obj.put(VERSION_FIELD, version);

		DBCollection col = db.getCollection(VERSION_COL);
		col.save(obj);
	}

	private int getVersion() {
		int version = 0;
		DBCollection col = db.getCollection(VERSION_COL);

		BasicDBObject obj = (BasicDBObject) col.findOne(new BasicDBObject(
				"_id", VERSION_ID));
		if (obj != null) {
			version = obj.getInt(VERSION_FIELD, 0);
		}
		return version;
	}

	private String loadVersionFile(int version) {
		InputStream is = getClass().getResourceAsStream(
				RESOURCE_BASE + version + RESOURCE_EXTENSION);
		StringWriter output = new StringWriter();
		try {
			if (is != null) {
				IOUtils.copy(is, output);
				return output.toString();
			}
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
		return null;
	}
}
