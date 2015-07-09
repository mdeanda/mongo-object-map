package com.thedeanda.mongo.test.domain;

import static org.junit.Assert.assertEquals;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.thedeanda.mongo.MongoSerialize;

public class TestCarSerializationNoDb {
	@Test
	public void testSingleNoDb() throws Exception {
		Car c = new Car();
		c.setId(new ObjectId());
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
	public void testInheritanceNoDb() throws Exception {
		Hexacar c = new Hexacar();
		c.setId(new ObjectId());
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
