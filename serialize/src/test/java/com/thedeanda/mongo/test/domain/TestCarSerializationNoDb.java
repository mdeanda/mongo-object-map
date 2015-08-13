package com.thedeanda.mongo.test.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.thedeanda.mongo.MongoSerialize;

public class TestCarSerializationNoDb {
	@Test
	public void testSingle() throws Exception {
		Car c = new Car();
		c.setId(new ObjectId());
		c.setType("Mercedes Benz");
		c.setModel("E350");
		c.setPrice(60000);

		MongoSerialize ms = new MongoSerialize();
		BasicDBObject o = ms.serialize(c);
		
		assertNotNull(o);
		assertEquals("Mercedes Benz", o.get("type"));
		assertEquals("E350", o.get("model"));
		assertEquals(60000, o.get("price"));
		
		Car c2 = new Car();
		ms.deserialize(o, c2);

		assertEquals("id:objectid", c.getId(), c2.getId());
		assertEquals("model:string", c.getModel(), c2.getModel());
		assertEquals("type:string", c.getType(), c2.getType());
		assertEquals("price:int", c.getPrice(), c2.getPrice());
	}

	@Test
	public void testInheritance() throws Exception {
		Hexacar c = new Hexacar();
		c.setId(new ObjectId());
		c.setType("Mercedes Benz");
		c.setModel("E350");
		c.setPrice(60000);
		c.setShape("hexagon");

		MongoSerialize ms = new MongoSerialize();
		BasicDBObject o = ms.serialize(c);
		
		assertNotNull(o);
		assertEquals("Mercedes Benz", o.get("type"));
		assertEquals("E350", o.get("model"));
		assertEquals(60000, o.get("price"));
		assertEquals("hexagon", o.get("shape"));
		
		Hexacar c2 = new Hexacar();
		ms.deserialize(o, c2);

		assertEquals("id:objectid", c.getId(), c2.getId());
		assertEquals("model:string", c.getModel(), c2.getModel());
		assertEquals("type:string", c.getType(), c2.getType());
		assertEquals("price:int", c.getPrice(), c2.getPrice());
		assertEquals("shape:hexagon", c.getShape(), c2.getShape());
	}

	@Test
	public void testObjectInField() throws Exception {
		CarDriver cd = new CarDriver();
		cd.setName("Miguel");
		Car c = new Car();
		c.setId(new ObjectId());
		c.setType("Mercedes Benz");
		c.setModel("E350");
		c.setPrice(60000);
		cd.setCar(c);

		MongoSerialize ms = new MongoSerialize();
		BasicDBObject o = ms.serialize(cd);
		
		assertNotNull(o);
		assertEquals("Miguel", o.get("name"));
		BasicDBObject co = (BasicDBObject) o.get("car");
		assertNotNull(co);
		assertEquals("Mercedes Benz", co.get("type"));
		assertEquals("E350", co.get("model"));
		assertEquals(60000, co.get("price"));
		
		CarDriver cd2 = new CarDriver();
		ms.deserialize(o, cd2);
		
		assertEquals("Miguel", cd2.getName());
		assertNotNull(cd2.getCar());
		Car c2 = cd2.getCar();
		assertEquals("Mercedes Benz", c2.getType());
		assertEquals("E350", c2.getModel());
		assertEquals(60000, c2.getPrice());
	}
	
	@Test
	public void testObjectInFieldNoNew() throws Exception {
		CarDriver cd = new CarDriver();
		cd.setName("Miguel");
		Car c = new Car();
		c.setId(new ObjectId());
		c.setType("Mercedes Benz");
		c.setModel("E350");
		c.setPrice(60000);
		cd.setCar(c);

		MongoSerialize ms = new MongoSerialize();
		BasicDBObject o = ms.serialize(cd);
		
		assertNotNull(o);
		assertEquals("Miguel", o.get("name"));
		BasicDBObject co = (BasicDBObject) o.get("car");
		assertNotNull(co);
		assertEquals("Mercedes Benz", co.get("type"));
		assertEquals("E350", co.get("model"));
		assertEquals(60000, co.get("price"));
		
		CarDriver cd2 = new CarDriver();
		Car c2 = new Car();
		cd2.setCar(c2);
		ms.deserialize(o, cd2);
		
		assertEquals("Miguel", cd2.getName());
		assertNotNull(cd2.getCar());
		assertEquals("car should be same instance we passed in", c2 , cd2.getCar());
		assertEquals("Mercedes Benz", c2.getType());
		assertEquals("E350", c2.getModel());
		assertEquals(60000, c2.getPrice());
	}
}
