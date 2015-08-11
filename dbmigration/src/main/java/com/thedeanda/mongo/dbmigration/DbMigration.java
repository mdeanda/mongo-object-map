package com.thedeanda.mongo.dbmigration;

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
	private static final ObjectId VERSION_ID = new ObjectId(0, 0, (short) 0, 0);
	private static final String VERSION_FIELD = "version";
	private static final String RESOURCE_EXTENSION = ".js";

	private static final Logger log = LoggerFactory
			.getLogger(DbMigration.class);
	private String versionCollectionName;
	private DB db;
	private String resourceBase;

	public DbMigration() {

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

		DBCollection col = db.getCollection(versionCollectionName);
		col.save(obj);
	}

	private int getVersion() {
		int version = 0;
		DBCollection col = db.getCollection(versionCollectionName);

		BasicDBObject obj = (BasicDBObject) col.findOne(new BasicDBObject(
				"_id", VERSION_ID));
		if (obj != null) {
			version = obj.getInt(VERSION_FIELD, 0);
		}
		return version;
	}

	private String loadVersionFile(int version) {
		InputStream is = getClass().getResourceAsStream(
				resourceBase + version + RESOURCE_EXTENSION);
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

	public String getVersionCollectionName() {
		return versionCollectionName;
	}

	public void setVersionCollectionName(String versionCollectionName) {
		this.versionCollectionName = versionCollectionName;
	}

	public DB getDb() {
		return db;
	}

	public void setDb(DB db) {
		this.db = db;
	}

	public String getResourceBase() {
		return resourceBase;
	}

	public void setResourceBase(String resourceBase) {
		this.resourceBase = resourceBase;
	}
}
