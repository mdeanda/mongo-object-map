package com.thedeanda.mongo.test.domain;

import static org.junit.Assert.assertEquals;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.thedeanda.mongo.MongoSerialize;
import com.thedeanda.mongo.MongoWrapper;

@Ignore
public class TestCarSerialization {
	protected DBCollection col;
	private MongoWrapper mw;

	@Before
	public void prepare() throws Exception {
		this.mw = new MongoWrapper();
		mw.setServers("localhost:27017");
		mw.setDbName("test");
		mw.init();
		this.col = mw.getDb().getCollection("test");
	}

	@After
	public void teardown() throws Exception {
		col.drop();
	}

	@Test
	public void testSingleDb() throws Exception {
		Car c = new Car();
		c.setId(new ObjectId());
		c.setType("Mercedes Benz");
		c.setModel("E350");
		c.setPrice(60000);

		MongoSerialize ms = new MongoSerialize();
		BasicDBObject o = ms.serialize(c);
		col.save(o);
		o = (BasicDBObject) col.findOne();
		Car c2 = new Car();
		ms.deserialize(o, c2);

		assertEquals("id:objectid", c.getId(), c2.getId());
		assertEquals("model:string", c.getModel(), c2.getModel());
		assertEquals("type:string", c.getType(), c2.getType());
		assertEquals("price:int", c.getPrice(), c2.getPrice());
	}

}
