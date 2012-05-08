package com.thedeanda.mongo.test.domain;

import static org.junit.Assert.assertEquals;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.thedeanda.mongo.MongoSerialize;
import com.thedeanda.mongo.MongoWrapper;

public class TestCarSerialization {
	protected DBCollection col;
	private MongoWrapper mw;

	@Before
	public void prepare() throws Exception {
		this.mw = new MongoWrapper("localhost", 27017, "test");
		this.col = mw.getDb().getCollection("test");
	}

	@After
	public void teardown() throws Exception {
		col.drop();
	}

	@Test
	public void testSingleNoDb() throws Exception {
		Car c = new Car();
		c.setId(new ObjectId(0, 0, 0));
		c.setType("Mercedes Benz");
		c.setModel("E350");
		c.setPrice(60000);

		MongoSerialize ms = new MongoSerialize();
		BasicDBObject o = ms.serialize(c);
		Car c2 = new Car();
		ms.deserialize(o, c2);

		assertEquals("id:objectid", c.getId(), c2.getId());
		assertEquals("model:string", c.getModel(), c2.getModel());
		assertEquals("type:string", c.getType(), c2.getType());
		assertEquals("price:int", c.getPrice(), c2.getPrice());
	}

	@Test
	public void testSingleDb() throws Exception {
		Car c = new Car();
		c.setId(new ObjectId(1, 2, 3));
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

	@Test
	public void testInheritanceNoDb() throws Exception {
		Hexacar c = new Hexacar();
		c.setId(new ObjectId(0, 0, 0));
		c.setType("Mercedes Benz");
		c.setModel("E350");
		c.setPrice(60000);
		c.setShape("hexagon");

		MongoSerialize ms = new MongoSerialize();
		BasicDBObject o = ms.serialize(c);
		Hexacar c2 = new Hexacar();
		ms.deserialize(o, c2);

		assertEquals("id:objectid", c.getId(), c2.getId());
		assertEquals("model:string", c.getModel(), c2.getModel());
		assertEquals("type:string", c.getType(), c2.getType());
		assertEquals("price:int", c.getPrice(), c2.getPrice());
		assertEquals("shape:hexagon", c.getShape(), c2.getShape());
	}
}
