package com.thedeanda.mongo;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

public class MongoSerialize {
	private static final Logger log = Logger.getLogger(MongoSerialize.class);

	private Map<Class<? extends Object>, Set<Field>> cache = new HashMap<Class<? extends Object>, Set<Field>>();

	public MongoSerialize() {

	}

	public void deserialize(BasicDBObject d, Object toObject)
			throws InstantiationException {
		if (d == null || toObject == null)
			return;

		Set<Field> fields = getFieldMap(toObject.getClass());
		for (Field fld : fields) {
			String name = fld.getName();
			boolean a = fld.isAccessible();
			fld.setAccessible(true);
			StoredField sf = fld.getAnnotation(StoredField.class);
			StoredIdField sif = fld.getAnnotation(StoredIdField.class);
			StoredDateField sdf = fld.getAnnotation(StoredDateField.class);
			StoredEnumField sef = fld.getAnnotation(StoredEnumField.class);

			if (sif != null && "id".equals(name)) {
				name = "_id";
			}
			Object value = d.get(name);
			try {
				if (sef != null && value != null) {
					fld.set(toObject, getEnumFromValue(fld, sef, value));
				} else if (sdf != null && value != null) {
					fld.set(toObject, value);
				} else if (sif != null && value != null) {
					if (value instanceof ObjectId
							&& fld.getType() == ObjectId.class)
						fld.set(toObject, value);
					else
						fld.set(toObject, value.toString());
				} else if (sf != null && value != null) {
					Class<?> fldClass = getClassFromName(fld.getType()
							.getCanonicalName());
					if (isSimpleClass(fldClass)) {
						fld.set(toObject, value);
					} else {
						Object toObject2 = fldClass.newInstance();
						deserialize((BasicDBObject) value, toObject2);
						fld.set(toObject, toObject2);
					}
				}
			} catch (IllegalArgumentException e) {
				log.error(e.getMessage(), e);
			} catch (IllegalAccessException e) {
				log.error(e.getMessage(), e);
			} catch (SecurityException e) {
				log.error(e.getMessage(), e);
			} catch (NoSuchFieldException e) {
				log.error(e.getMessage(), e);
			} finally {
				fld.setAccessible(a);
			}
		}
	}

	private Class<? extends Object> getClassFromName(String name) {
		try {
			if ("boolean".equals(name)) {
				return Boolean.class;
			} else if ("int".equals(name)) {
				return Integer.class;
			} else if ("long".equals(name)) {
				return Long.class;
			} else if ("float".equals(name)) {
				return Float.class;
			} else if ("double".equals(name)) {
				return Double.class;
			} else
				return Class.forName(name);
		} catch (ClassNotFoundException e) {
			log.warn(e.getMessage(), e);
		}
		return null;
	}

	public BasicDBObject serialize(Object object) {
		Set<Field> fields = getFieldMap(object.getClass());
		BasicDBObject ret = new BasicDBObject();
		for (Field fld : fields) {
			try {
				boolean a = fld.isAccessible();
				fld.setAccessible(true);
				Object value = fld.get(object);
				fld.setAccessible(a);
				boolean sf = fld.getAnnotation(StoredField.class) != null;
				boolean sif = fld.getAnnotation(StoredIdField.class) != null;
				boolean sdf = fld.getAnnotation(StoredDateField.class) != null;
				StoredEnumField sefAnot = fld
						.getAnnotation(StoredEnumField.class);
				boolean sef = sefAnot != null;

				if (!isAcceptableClass(fld.getDeclaringClass()))
					throw new ClassCastException(
							"Class must be concrete and not abstract or interface");

				String name = fld.getName();
				if (sif && "id".equals(name)) {
					name = "_id";
				}
				if (value != null && sdf) {
					ret.put(name, value);
				} else if (value != null && sif) {
					if (value instanceof ObjectId)
						ret.put(name, value);
					else
						ret.put(name, new ObjectId(String.valueOf(value)));
				} else if (value != null && sf) {
					// check if simple class to use string
					if (isSimpleClass(value.getClass())) {
						ret.put(name, value);
					} else {
						ret.put(name, serialize(value));
					}
				} else if (value != null && sef) {
					ret.put(name, enumValue(sefAnot, fld, ret, value));
				} else if (sf || sif || sdf || sef) {
					ret.put(name, null);
				}
			} catch (IllegalArgumentException e) {
				log.error(e.getMessage(), e);
			} catch (IllegalAccessException e) {
				log.error(e.getMessage(), e);
			} catch (SecurityException e) {
				log.error(e.getMessage(), e);
			} catch (NoSuchFieldException e) {
				log.error(e.getMessage(), e);
			}
		}
		return ret;
	}

	private Object getEnumFromValue(Field fld, StoredEnumField sef, Object value)
			throws IllegalArgumentException, IllegalAccessException,
			SecurityException, NoSuchFieldException {
		Object retVal = null;
		Class<?> enumClass = fld.getType();
		Object[] enums = enumClass.getEnumConstants();
		if (enums != null) {
			Field enumField = enumClass.getDeclaredField(sef.value());
			boolean acc = enumField.isAccessible();
			enumField.setAccessible(true);
			for (Object o : enums) {
				Object val = enumField.get(o);
				if (val.equals(value)) {
					retVal = o;
					break;
				}
			}
			enumField.setAccessible(acc);
		}
		return retVal;
	}

	private Object enumValue(StoredEnumField sefAnot, Field fld,
			BasicDBObject ret, Object value) throws SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		@SuppressWarnings("unused")
		Field[] tmp = fld.getClass().getEnumConstants();

		Field enumField = value.getClass().getDeclaredField(sefAnot.value());
		boolean acc = enumField.isAccessible();
		enumField.setAccessible(true);
		Object valueToStore = enumField.get(value);
		enumField.setAccessible(acc);

		return valueToStore;
	}

	private boolean isAcceptableClass(Class<?> cls) {
		if (cls.isInterface() || Modifier.isAbstract(cls.getModifiers()))
			return false;
		else
			return true;
	}

	private boolean isSimpleClass(Class<? extends Object> cls) {
		if (cls.isPrimitive() || cls.isAssignableFrom(String.class)
				|| cls.isAssignableFrom(Number.class)
				|| cls.isAssignableFrom(Boolean.class)
				|| cls.isAssignableFrom(Integer.class)
				|| cls.isAssignableFrom(Long.class)
				|| cls.isAssignableFrom(Float.class)
				|| cls.isAssignableFrom(Double.class))
			return true;
		else
			return false;
	}

	private Set<Field> getFieldMap(Class<? extends Object> cls) {
		Class<? extends Object> key = cls;
		if (!cache.containsKey(key)) {
			Set<Field> fields = new HashSet<Field>();

			while (cls != null) {
				Field[] flds = cls.getDeclaredFields();
				for (Field fld : flds) {
					if (fld.isAnnotationPresent(StoredField.class)
							// /||
							// fld.isAnnotationPresent(StoredEnumField.class)
							|| fld.isAnnotationPresent(StoredIdField.class)
							|| fld.isAnnotationPresent(StoredDateField.class)) {
						fields.add(fld);
					}
					if (fld.isAnnotationPresent(StoredEnumField.class)) {
						fields.add(fld);
					}
				}
				cls = cls.getSuperclass();
			}
			cache.put(key, fields);
		}
		return cache.get(key);
	}
}
