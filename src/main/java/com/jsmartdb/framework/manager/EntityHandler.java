/*
 * JSmartDB - Java ORM Framework
 * Copyright (c) 2014, Jeferson Albino da Silva, All rights reserved.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
*/

package com.jsmartdb.framework.manager;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.jsmartdb.framework.types.CascadeType;
import com.jsmartdb.framework.types.TableType;

import com.jsmartdb.framework.annotation.Column;
import com.jsmartdb.framework.annotation.Id;
import com.jsmartdb.framework.annotation.JoinId;
import com.jsmartdb.framework.annotation.ManyToMany;
import com.jsmartdb.framework.annotation.OneToMany;
import com.jsmartdb.framework.annotation.OneToOne;
import com.jsmartdb.framework.annotation.QueryFilter;
import com.jsmartdb.framework.annotation.QueryFilters;
import com.jsmartdb.framework.annotation.Table;
import com.jsmartdb.framework.annotation.Transient;

/*package*/ final class EntityHandler {

	/*package*/ static void setInternalId(Entity entity) {
		entity.internalId = UUID.randomUUID().toString();
	}

	/*package*/ static Table getTable(final Class<?> clazz) {
		return clazz.getAnnotation(Table.class);
	}

	/*package*/ static boolean isJoinTable(final Class<?> clazz) {
		Table table = getTable(clazz);
		return table != null && table.type() == TableType.JOIN_TABLE;
	}

	/*package*/ static Id getId(final Field field) {
		return field.getAnnotation(Id.class);
	}

	/*package*/ static JoinId getJoinId(final Field field) {
		return field.getAnnotation(JoinId.class);
	}

	/*package*/ static Column getColumn(final Field field) {
		return field.getAnnotation(Column.class);
	}

	/*package*/ static String getRefererName(final Field field) {
		Id id = getId(field);
		if (id != null) {
			return id.name();
		}
		Column column = getColumn(field);
		if (column != null) {
			return column.name();
		}
		return null;
	}

	/*package*/ static OneToOne getOneToOne(final Field field) {
		return field.getAnnotation(OneToOne.class);
	}

	/*package*/ static OneToMany getOneToMany(final Field field) {
		return field.getAnnotation(OneToMany.class);
	}

	/*package*/ static ManyToMany getManyToMany(final Field field) {
		return field.getAnnotation(ManyToMany.class);
	}

	/*package*/ static ManyToMany getManyToMany(final Field[] fields, final Class<?> clazz) {
		for (Field field : fields) {
			if (field.getType() == Set.class || field.getType() == List.class) {

				ManyToMany manyToMany = getManyToMany(field);
				if (manyToMany != null && getGenericType(field) == clazz) {
					return manyToMany;
				}
			}
		}
		return null;
	}

	/*package*/ static QueryFilters getQueryFilters(final Class<?> clazz) {
		return clazz.getAnnotation(QueryFilters.class);
	}

	/*package*/ static QueryFilter getQueryFilter(final Class<?> clazz) {
		return clazz.getAnnotation(QueryFilter.class);
	}

	/*package*/ static QueryFilter getQueryFilter(final QueryParam param) {
    	QueryFilter queryFilter = null;
    	QueryFilters queryFilters = getQueryFilters(param.getEntityClazz());

    	if (queryFilters != null && queryFilters.filters().length > 0) {
    		for (QueryFilter filter : queryFilters.filters()) {
    			if (param.getFilterName().equals(filter.name())) {
    				if (queryFilter != null) {
    					throw new RuntimeException("QueryFilter name cannot be duplicated in the same Entity: " + param.getFilterName());
    				}
    				queryFilter = filter;
    			}
    		}
    	}

    	if (queryFilter == null) {
    		queryFilter = getQueryFilter(param.getEntityClazz());
    	}

    	if (queryFilter == null || queryFilter.name() == null || !param.getFilterName().equals(queryFilter.name()) 
				|| queryFilter.where() == null || queryFilter.where().trim().isEmpty()) {
			throw new RuntimeException("QueryFilter for custom query is invalid! " + param.getFilterName());
		}
    	return queryFilter;
	}

	/*package*/ static Object getValue(final Entity entity, final Field field) {
		if (field != null) {
			try {
				field.setAccessible(true);
				return field.get(entity);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
		return null;
	}

	/*package*/ static void setValue(final Entity entity, final Field field, final Object value) {
		if (field != null) {
			try {
				field.setAccessible(true);
				field.set(entity, value);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			} 
		}
	}
 
	@SuppressWarnings("unchecked")
	/*package*/ static void addValue(final Entity entity, final Field field, final Object value) {
		if (field != null) {
			try {
				Collection<Entity> collection = (Collection<Entity>) getValue(entity, field);
				if (collection != null) {
					collection.add((Entity) value);
				}
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	/*package*/ static Class<?> getGeneratedIdClass(final Entity entity) {
		for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {

			Id id = field.getAnnotation(Id.class);
			if (id != null && id.generated()) {
				return field.getType();
			}

			Column column = field.getAnnotation(Column.class);
			if (column != null && column.incremental()) {
				return field.getType();
			}
		}
		return null;
	}

	/*package*/ static String getFirstIdName(final Class<?> clazz) {
		for (Field field : EntityFieldsMapper.getFields(clazz)) {
			Id id = field.getAnnotation(Id.class);
			if (id != null) {
				return id.name();
			}
		}
		return null;
	}

	/*package*/ static Object getRefererValue(final Entity entity, final Field field) {
		Id id = getId(field);
		if (id != null) {
			return getValue(entity, field);
		}
		Column column = getColumn(field);
		if (column != null) {
			return getValue(entity, field);
		}
		return null;
	}

	/*package*/ static List<Object> getIdValues(final Entity entity) {
		final List<Object> idValues = new ArrayList<Object>();
		for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
			Id id = field.getAnnotation(Id.class);
			if (id != null) {
				idValues.add(getValue(entity, field));
			}
		}
		return idValues;
	}

	/*package*/ static void setGeneratedValue(final Entity entity, final Object value) {
		for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {

			Id id = field.getAnnotation(Id.class);
			if (id != null && id.generated()) {
				setValue(entity, field, value);
				return;
			}

			Column column = field.getAnnotation(Column.class);
			if (column != null && column.incremental()) {
				setValue(entity, field, value);
				return;
			}
		}
	}

	@SuppressWarnings("unchecked")
	/*package*/ static Entity getCollectionValue(final Entity entity, final Field field, final Object id, final Field refererField) {
		final Collection<Entity> collection = (Collection<Entity>) getValue(entity, field);
		if (collection != null) {
			for (Entity obj : collection) {
				if (obj != null && getRefererValue(obj, refererField).equals(id)) {
					return obj;
				}
			}
		}
		return null;
	}

	/*package*/ static String getFirstJoinIdName(final Class<?> clazz) {
		for (Field field : EntityFieldsMapper.getFields(clazz)) {

			JoinId joinId = field.getAnnotation(JoinId.class);
			if (joinId != null) {
				return joinId.column();
			}
		}
		return null;
	}

	/*package*/ static List<Entity> getJoinedTos(final Entity entity, final Class<?> clazz) {
		final List<Entity> joinedToList = new ArrayList<Entity>();

		for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
			JoinId joinId = getJoinId(field);

			if (joinId != null && field.getType() != clazz) {
				joinedToList.add((Entity) getValue(entity, field));
			}
		}
		return joinedToList;
	}

	/*package*/ static List<Entity> getJoinIdList(final Entity entity, final CascadeType cascadeType) {
		final List<Entity> joinIdList = new ArrayList<Entity>();
	    if (!EntityContext.isBlockCascade()) {

	        for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
	        	JoinId joinId = getJoinId(field);

	            if (joinId != null && containsCascadeType(joinId.cascade(), cascadeType)) {
	            	joinIdList.add((Entity) getValue(entity, field));
	            }
	        }
	    }
	    return joinIdList;
	}

	/*package*/ static void getJoinIdMap(final Entity entity, final CascadeType cascade, final Map<Class<?>, List<Entity>> joinIdMap) {
		if (!EntityContext.isBlockCascade()) {

			for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
	        	JoinId joinId = getJoinId(field);

	            if (joinId != null && containsCascadeType(joinId.cascade(), cascade)) {
	            	Entity ent = (Entity) getValue(entity, field);
	            	if (ent != null) {
		            	if (!joinIdMap.containsKey(ent.getClass())) {
		            		joinIdMap.put(ent.getClass(), new ArrayList<Entity>());
		            	}

		            	joinIdMap.get(ent.getClass()).add(ent);
	            	}
	            }
	        }
		}
	}

	/*package*/ static List<Entity> getOneToOneList(final Entity entity, final CascadeType cascadeType) {
		final List<Entity> oneToOneList = new ArrayList<Entity>();
	    if (!EntityContext.isBlockCascade()) {

	        for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
	            OneToOne oneToOne = getOneToOne(field);

	            if (oneToOne != null && containsCascadeType(oneToOne.cascade(), cascadeType)) {
	            	oneToOneList.add((Entity) getValue(entity, field));
	            }
	        }
	    }
	    return oneToOneList;
	}

	/*package*/ static void getOneToOneMap(final Entity entity, final CascadeType cascadeType, final Map<Class<?>, List<Entity>> oneToOneMap) {
		if (!EntityContext.isBlockCascade()) {
			for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
	            OneToOne oneToOne = getOneToOne(field);
	            if (oneToOne != null && containsCascadeType(oneToOne.cascade(), cascadeType)) {

	            	Entity ent = (Entity) getValue(entity, field);
	            	if (ent != null) {
		            	if (!oneToOneMap.containsKey(ent.getClass())) {
		            		oneToOneMap.put(ent.getClass(), new ArrayList<Entity>());
		            	}

		            	oneToOneMap.get(ent.getClass()).add(ent);
	            	}
	            }
	        }
		}
	}

	@SuppressWarnings("unchecked")
	/*package*/ static List<Collection<Entity>> getOneToManyList(final Entity entity, final CascadeType cascadeType, final boolean isManyToMany) {
		final List<Collection<Entity>> oneToManyList = new ArrayList<Collection<Entity>>();
		if (!EntityContext.isBlockCascade()) {

			for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
				OneToMany oneToMany = getOneToMany(field);

				if (oneToMany != null) {
					Class<?> oneToManyClass = getGenericType(field);

					if (isManyToMany) {
						if (getTable(oneToManyClass).type() == TableType.JOIN_TABLE) {

							oneToManyList.add((Collection<Entity>) getValue(entity, field));

							// Set many to many cascade flag
							if (containsCascadeType(oneToMany.cascade(), cascadeType)) {
								EntityContext.addManyToManyCascade(entity.getClass(), oneToManyClass);
		                    }
						}

					} else {
						if (getTable(oneToManyClass).type() == TableType.DEFAULT_TABLE) {
							try {
								if (containsCascadeType(oneToMany.cascade(), cascadeType)) {

									Collection<Entity> collection = (Collection<Entity>) getValue(entity, field);

									Object entityIdValue = getRefererValue(entity, EntityFieldsMapper.getField(entity.getClass(), oneToMany.joinColumn().column()));

									// Cascade its id value to referenced value on OneToMany 
									for (Entity obj : collection) {
										setValue(obj, EntityFieldsMapper.getField(obj.getClass(), oneToMany.joinColumn().referer()), entityIdValue);
									}
									oneToManyList.add(collection);
								}
							} catch (Exception ex) {
								throw new RuntimeException(ex); 
							}
						}
					}
				}
			}
		}
		return oneToManyList;
	}

	 @SuppressWarnings("unchecked")
	 /*package*/ static void getOneToManyMap(final Entity entity, final CascadeType cascadeType, final boolean isManyToMany, final Map<Class<?>, List<EntityBatch>> oneToManyMap) {
		if (!EntityContext.isBlockCascade()) {

			for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
				OneToMany oneToMany = getOneToMany(field);

				if (oneToMany != null) {

					Class<?> oneToManyClass = getGenericType(field);
					Collection<Entity> collection = (Collection<Entity>) getValue(entity, field);

					if (!oneToManyMap.containsKey(oneToManyClass)) {
						oneToManyMap.put(oneToManyClass, new ArrayList<EntityBatch>());
					}

					if (isManyToMany) {
						if (getTable(oneToManyClass).type() == TableType.JOIN_TABLE) {

							oneToManyMap.get(oneToManyClass).add(new EntityBatch(entity, collection));

							// Set many to many cascade flag
							if (containsCascadeType(oneToMany.cascade(), cascadeType)) {
								EntityContext.addManyToManyCascade(entity.getClass(), oneToManyClass);
		                    }
						}

					} else {
						if (getTable(oneToManyClass).type() == TableType.DEFAULT_TABLE) {
							
							oneToManyMap.get(oneToManyClass).add(new EntityBatch(entity, collection));

							try {
								if (containsCascadeType(oneToMany.cascade(), cascadeType)) {

									Object entityIdValue = getRefererValue(entity, EntityFieldsMapper.getField(entity.getClass(), oneToMany.joinColumn().column()));

									// Cascade its id value to referenced value on OneToMany 
									for (Entity obj : collection) {
										setValue(obj, EntityFieldsMapper.getField(obj.getClass(), oneToMany.joinColumn().referer()), entityIdValue);
									}
								}
							} catch (Exception ex) {
								throw new RuntimeException(ex); 
							}
						}
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	/*package*/ static List<EntityManyToMany> getManyToManyList(final Entity entity, final CascadeType cascadeType) {
		final List<EntityManyToMany> manyToManyList = new ArrayList<EntityManyToMany>();
	    if (!EntityContext.isBlockCascade()) {

            for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
                ManyToMany manyToMany = getManyToMany(field);

                if (manyToMany != null) {
                	Class<?> manyToManyClass = getGenericType(field);

                	EntityManyToMany entityManyToMany = new EntityManyToMany();
                	entityManyToMany.setManyToMany(manyToMany);
                	entityManyToMany.setCollection((Collection<Entity>) getValue(entity, field));

                	manyToManyList.add(entityManyToMany);

                	// Set many to many cascade flag
                    if (containsCascadeType(manyToMany.cascade(), cascadeType)) {
                    	EntityContext.addManyToManyCascade(entity.getClass(), manyToManyClass);
                    }
                }
            }
        }
        return manyToManyList;
	}

	@SuppressWarnings("unchecked")
	/*package*/ static void getManyToManyMap(final Entity entity, final CascadeType cascadeType, final Map<Class<?>, List<EntityBatch>> manyToManyMap) {
		if (!EntityContext.isBlockCascade()) {

            for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
                ManyToMany manyToMany = getManyToMany(field);

                if (manyToMany != null) {
                	Class<?> manyToManyClass = getGenericType(field);

                	if (!manyToManyMap.containsKey(manyToManyClass)) {
                		manyToManyMap.put(manyToManyClass, new ArrayList<EntityBatch>());
                	}

                	EntityBatch entityBatch = new EntityBatch(entity, (Collection<Entity>) getValue(entity, field));
                	entityBatch.setManyToMany(manyToMany);
                	manyToManyMap.get(manyToManyClass).add(entityBatch);

                	// Set many to many cascade flag
                    if (containsCascadeType(manyToMany.cascade(), cascadeType)) {
                    	EntityContext.addManyToManyCascade(entity.getClass(), manyToManyClass);
                    }
                }
            }
        }
	}

	/*package*/ static List<Entity> getEntitiesToInsert(final Collection<Entity> entities) {
		final List<Entity> insertEntities = new ArrayList<Entity>();
		Iterator<Entity> updateEntities = entities.iterator();

		while (updateEntities.hasNext()) {
			Entity entity = updateEntities.next();

			if (entity.internalId == null) {
				updateEntities.remove();
				insertEntities.add(entity);
			}
		}
		return insertEntities;
	}

	/*package*/ static Class<?> getGenericType(final Field field) {
		return (Class<?>) ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
	}

	/*package*/ static boolean isId(final Field field) {
		return field.getAnnotation(Id.class) != null;
	}

	/*package*/ static boolean isColumn(final Field field) {
		return field.getAnnotation(Column.class) != null;
	}

	/*package*/ static boolean isOneToMany(final Field field) {
		return field.getAnnotation(OneToMany.class) != null;
	}

	/*package*/ static boolean isManyToMany(final Field field) {
		return field.getAnnotation(ManyToMany.class) != null;
	}

	/*package*/ static boolean isReserved(final Field field) {
		return Modifier.isStatic(field.getModifiers());
	}

	/*package*/ static boolean isTransient(final Field field) {
		return field.getAnnotation(Transient.class) != null;
	}

	@SuppressWarnings("unchecked")
	/*package*/ static boolean containsObject(final Entity entity, final Field field, final Object object) {
		final Collection<Entity> collection = (Collection<Entity>) getValue(entity, field);
		if (collection != null) {
			return collection.contains(object);
		}
		return false;
	}

	/*package*/ static void validateColumns(final Entity entity) {
		for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
			if (isColumn(field)) {

				if (field.getType() == String.class) {
					int length = getColumn(field).length();
					if (length != Column.NO_LENGTH) {

						String value = (String) getValue(entity, field);
						if (value != null && value.length() > length) {
							throw new RuntimeException("Length of string value " + value + "for variable " + field.getName() + " must be less or equal to " + length + "!");
						}
					}
				}
			}
		}
	}

	/*package*/ static boolean containsCascadeType(final CascadeType[] cascadeTypes, final CascadeType cascadeType) {
        if (cascadeTypes != null) {
        	for (CascadeType cascade : cascadeTypes) {
        		if (cascade == cascadeType) {
        			return true;
        		}
        	}
        }
        return false;
	}

}