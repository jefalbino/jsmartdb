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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.jsmartdb.framework.types.CascadeType;

import com.jsmartdb.framework.annotation.ManyToMany;

/*package*/ final class EntityBatchRepository {

	private static Logger LOGGER = Logger.getLogger(EntityBatchRepository.class.getPackage().getName());

	private static Boolean LOG_SQL = new Boolean(EntityPersistence.getInstance().getProperty(EntityPersistence.SQL_SHOW_SQL));

	private static final int GENERATED_KEY_INDEX = 1;

	/*package*/ void insert(final Collection<? extends Entity> entities, boolean blockCascade) {
		if (entities != null && !entities.isEmpty()) {
			EntityContext.setBlockCascade(blockCascade);
			if (EntityHandler.isJoinTable(entities.iterator().next().getClass())) {
				insertJoin(entities);
			} else {
				insertDefault(entities);
			}
		} else {
			throw new RuntimeException("Collection of entities to be inserted cannot be null or empty!");
		}
		EntityContext.clearBuildBlockedClasses();
	}

	private void insertDefault(final Collection<? extends Entity> entities) {

		final Map<Class<?>, List<Entity>> oneToOneMap = new HashMap<Class<?>, List<Entity>>();
		final Map<Class<?>, List<EntityBatch>> oneToManyMap = new HashMap<Class<?>, List<EntityBatch>>();
		final Map<Class<?>, List<EntityBatch>> manyToManyMap = new HashMap<Class<?>, List<EntityBatch>>();
		final Map<Class<?>, List<EntityBatch>> oneToManyToManyMap = new HashMap<Class<?>, List<EntityBatch>>();

		// Validate entity and collect OneToOne relations
		for (Entity entity : entities) {
			EntityHandler.validateColumns(entity);

        	// Get all references to OneToOne from all entities
        	EntityHandler.getOneToOneMap(entity, CascadeType.INSERT, oneToOneMap);
		}

		// Cascade for OneToOne references
		for (Class<?> clazz : oneToOneMap.keySet()) {
			List<Entity> oneToOneList = oneToOneMap.get(clazz);
			if (!oneToOneList.isEmpty()) {
				insertDefault(oneToOneList);
			}
		}

		// Insert current references
		insertBatch(entities);

		// After entities (current references) insertion with its ids, it is possible to collect another relations
		for (Entity entity : entities) {

        	// Get all references to OneToMany from all entities
        	EntityHandler.getOneToManyMap(entity, CascadeType.INSERT, false, oneToManyMap);

        	// Get all references to ManyToMany from all entities
        	EntityHandler.getManyToManyMap(entity, CascadeType.INSERT, manyToManyMap);

            // Get all references to OneToMany related to ManyToMany from all entities
        	EntityHandler.getOneToManyMap(entity, CascadeType.INSERT, true, oneToManyToManyMap);
		}

		// Cascade for OneToMany references
		for (Class<?> clazz : oneToManyMap.keySet()) {
			for (EntityBatch entityBatch : oneToManyMap.get(clazz)) {
				Collection<Entity> entityCollection = entityBatch.getCollection();
				if (!entityCollection.isEmpty()) {
					insertDefault(entityCollection);
				}
			}
		}

		// Cascade for ManyToMany references
		for (Class<?> clazz : manyToManyMap.keySet()) {
			for (EntityBatch entityBatch : manyToManyMap.get(clazz)) {
				Collection<Entity> entityCollection = entityBatch.getCollection();

				if (!entityCollection.isEmpty()) {
					insertDefault(entityCollection);
					
					insertJoin(entityBatch.getManyToMany(), entityBatch.getEntity(), entityCollection);
				}
			}
		}

		// Cascade for OneToMany references related to ManyToMany
		for (Class<?> clazz : oneToManyToManyMap.keySet()) {
			for (EntityBatch entityBatch : oneToManyToManyMap.get(clazz)) {

				Collection<Entity> entityCollection = entityBatch.getCollection();
				if (!entityCollection.isEmpty()) {

	    			List<Entity> joinedEntityList = new ArrayList<Entity>(); 
	    			for (Entity ent : entityCollection) {
	    				joinedEntityList.addAll(EntityHandler.getJoinedTos(ent, entityBatch.getEntity().getClass()));
	    			}
	
	    			if (!joinedEntityList.isEmpty()) {
	    				EntityContext.addBuildBlockedClass(joinedEntityList.get(0).getClass());
	    				insertDefault(joinedEntityList);
	    			}
	
	    			EntityContext.addBuildBlockedClass(entityBatch.getEntity().getClass());
					insertJoin(entityCollection);
				}
			}
		}
	}

	private void insertBatch(final Collection<? extends Entity> entities) {
		try {
			long batchCount = 0;
			PreparedStatement pstmt = null;
			List<Object> generatedKeys = new ArrayList<Object>();

			for (Entity entity : entities) {
				if (pstmt == null) {
					pstmt = EntityUpdate.getInsert(entity.getClass());
					if (LOG_SQL) {
						LOGGER.log(Level.INFO, pstmt.toString());
		    		}
				}

				EntityUpdate.putInsertValues(pstmt, entity);
				pstmt.addBatch();

				 if (++batchCount % ConnectionFactory.BATCH_SIZE == 0 || batchCount == entities.size()) {
					 
					 Class<?> generatedIdClass = EntityHandler.getGeneratedIdClass(entity);
					 List<Object> ids = executeUpdate(pstmt, generatedIdClass, batchCount == entities.size());

					 if (generatedIdClass != null) {
						 generatedKeys.addAll(ids);
					 }
				 }
			}

			// Insert generated ids into entities
			int idCount = 0;
			for (Entity entity : entities) {
				EntityHandler.setInternalId(entity);

				if (!generatedKeys.isEmpty()) {
					EntityHandler.setGeneratedValue(entity, generatedKeys.get(idCount++));
				}
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	private void insertJoin(final Collection<? extends Entity> joinEntities) {
		try {
			final Map<Class<?>, List<Entity>> joinIdMap = new HashMap<Class<?>, List<Entity>>();

			// Validate entity and collect JoinId relations
			for (Entity joinEntity : joinEntities) {
				EntityHandler.validateColumns(joinEntity);

	        	// Get all references to JoinId from all entities
	        	EntityHandler.getJoinIdMap(joinEntity, CascadeType.INSERT, joinIdMap);
			}

			// Cascade for JoinId references
			for (Class<?> clazz : joinIdMap.keySet()) {
				List<Entity> joinIdList = joinIdMap.get(clazz);

				if (!joinIdList.isEmpty() && !EntityContext.containsBuildBlockedClass(clazz)) {
					insertDefault(joinIdList);
				}
			}

			long batchCount = 0;
			PreparedStatement pstmt = null;

			for (Entity joinEntity : joinEntities) {
				if (pstmt == null) {
					pstmt = EntityUpdate.getInsertJoin(joinEntity.getClass());
					if (LOG_SQL) {
						LOGGER.log(Level.INFO, pstmt.toString());
		    		}
				}

				EntityUpdate.putInsertJoinValues(pstmt, joinEntity);
				pstmt.addBatch();

				if (++batchCount % ConnectionFactory.BATCH_SIZE == 0 || batchCount == joinEntities.size()) {
					executeUpdate(pstmt, null, batchCount == joinEntities.size());
				}
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	private void insertJoin(final ManyToMany manyToMany, final Entity entityOne, final Collection<? extends Entity> entitiesTwo) {
		try {
			long batchCount = 0;
			PreparedStatement pstmt = null;

			for (Entity entityTwo : entitiesTwo) {
				if (pstmt == null) {
					pstmt = EntityUpdate.getInsertJoin(entityOne.getClass(), entityTwo.getClass());
					if (LOG_SQL) {
						LOGGER.log(Level.INFO, pstmt.toString());
		    		}
				}

				EntityUpdate.putInsertJoinValues(pstmt, manyToMany, entityOne, entityTwo);
				pstmt.addBatch();

				if (++batchCount % ConnectionFactory.BATCH_SIZE == 0 || batchCount == entitiesTwo.size()) {
					executeUpdate(pstmt, null, batchCount == entitiesTwo.size());
				}
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ void update(final Collection<? extends Entity> entities, boolean blockCascade) {
		if (entities != null && !entities.isEmpty()) {
			EntityContext.setBlockCascade(blockCascade);
			if (EntityHandler.isJoinTable(entities.iterator().next().getClass())) {
				updateJoin(entities);
			} else {
				updateDefault(entities);
			}
		} else {
			throw new RuntimeException("Collection of entities to be updated cannot be null or empty!");
		}
		EntityContext.clearBuildBlockedClasses();
	}

	private void updateDefault(final Collection<? extends Entity> entities) {

		final Map<Class<?>, List<Entity>> oneToOneMap = new HashMap<Class<?>, List<Entity>>();
		final Map<Class<?>, List<EntityBatch>> oneToManyMap = new HashMap<Class<?>, List<EntityBatch>>();
		final Map<Class<?>, List<EntityBatch>> manyToManyMap = new HashMap<Class<?>, List<EntityBatch>>();
		final Map<Class<?>, List<EntityBatch>> oneToManyToManyMap = new HashMap<Class<?>, List<EntityBatch>>();

		// Validate entity and collect update relations
		for (Entity entity : entities) {
			EntityHandler.validateColumns(entity);

        	// Get all references to OneToOne from all entities
        	EntityHandler.getOneToOneMap(entity, CascadeType.UPDATE, oneToOneMap);

        	// Get all references to OneToMany from all entities
        	EntityHandler.getOneToManyMap(entity, CascadeType.UPDATE, false, oneToManyMap);

        	// Get all references to ManyToMany from all entities
        	EntityHandler.getManyToManyMap(entity, CascadeType.UPDATE, manyToManyMap);

            // Get all references to OneToMany related to ManyToMany from all entities
        	EntityHandler.getOneToManyMap(entity, CascadeType.UPDATE, true, oneToManyToManyMap);
		}

		// Cascade for OneToOne references
		for (Class<?> clazz : oneToOneMap.keySet()) {
			List<Entity> oneToOneList = oneToOneMap.get(clazz);

			if (!oneToOneList.isEmpty()) {

				List<Entity> insertEntities = EntityHandler.getEntitiesToInsert(oneToOneList);
				if (!insertEntities.isEmpty()) {
					insertDefault(insertEntities);
				}
			
				updateDefault(oneToOneList);
			}
		}

		// Insert current references
		updateBatch(entities);

		// Cascade for OneToMany references
		for (Class<?> clazz : oneToManyMap.keySet()) {
			for (EntityBatch entityBatch : oneToManyMap.get(clazz)) {
				Collection<Entity> entityCollection = entityBatch.getCollection();

				if (!entityCollection.isEmpty()) {

					List<Entity> insertEntities = EntityHandler.getEntitiesToInsert(entityCollection);
					if (!insertEntities.isEmpty()) {
						insertDefault(insertEntities);
					}

					updateDefault(entityCollection);
				}
			}
		}

		// Cascade for ManyToMany references
		for (Class<?> clazz : manyToManyMap.keySet()) {
			for (EntityBatch entityBatch : manyToManyMap.get(clazz)) {
				Collection<Entity> entityCollection = entityBatch.getCollection();

				if (!entityCollection.isEmpty()) {

					List<Entity> insertEntities = EntityHandler.getEntitiesToInsert(entityCollection);
					if (!insertEntities.isEmpty()) {
						insertDefault(insertEntities);
					}

					updateDefault(entityCollection);
				}
			}
		}

		// Cascade for OneToMany references related to ManyToMany
		for (Class<?> clazz : oneToManyToManyMap.keySet()) {
			for (EntityBatch entityBatch : oneToManyToManyMap.get(clazz)) {
				Collection<Entity> entityCollection = entityBatch.getCollection();

				if (!entityCollection.isEmpty()) {

	    			List<Entity> joinedEntityList = new ArrayList<Entity>(); 
	    			for (Entity ent : entityCollection) {
	    				joinedEntityList.addAll(EntityHandler.getJoinedTos(ent, entityBatch.getEntity().getClass()));
	    			}
	
	    			if (!joinedEntityList.isEmpty()) {
		    			List<Entity> insertEntities = EntityHandler.getEntitiesToInsert(joinedEntityList);
						if (!insertEntities.isEmpty()) {
							insertDefault(insertEntities);
						}
						EntityContext.addBuildBlockedClass(joinedEntityList.get(0).getClass());
		    			updateDefault(joinedEntityList);
	    			}

	    			EntityContext.addBuildBlockedClass(entityBatch.getEntity().getClass());
					updateJoin(entityCollection);
				}
			}
		}
	}

	private void updateJoin(final Collection<? extends Entity> joinEntities) {
		try {
			final Map<Class<?>, List<Entity>> joinIdMap = new HashMap<Class<?>, List<Entity>>();

			// Validate entity and collect JoinId relations
			for (Entity joinEntity : joinEntities) {
				EntityHandler.validateColumns(joinEntity);

	        	// Get all references to JoinId from all entities
	        	EntityHandler.getJoinIdMap(joinEntity, CascadeType.UPDATE, joinIdMap);
			}

			// Cascade for JoinId references
			for (Class<?> clazz : joinIdMap.keySet()) {
				List<Entity> joinIdList = joinIdMap.get(clazz);

				if (!joinIdList.isEmpty() && !EntityContext.containsBuildBlockedClass(clazz)) {
					List<Entity> insertEntities = EntityHandler.getEntitiesToInsert(joinIdList);
					if (!insertEntities.isEmpty()) {
						insertDefault(insertEntities);
					}
					updateDefault(joinIdList);
				}
			}

			long batchCount = 0;
			PreparedStatement pstmt = null;

			for (Entity joinEntity : joinEntities) {
				if (pstmt == null) {
					pstmt = EntityUpdate.getUpdateJoin(joinEntity.getClass());
					if (LOG_SQL) {
						LOGGER.log(Level.INFO, pstmt.toString());
					}
				}

				EntityUpdate.putUpdateJoinValues(pstmt, joinEntity);
				pstmt.addBatch();

				if (++batchCount % ConnectionFactory.BATCH_SIZE == 0 || batchCount == joinEntities.size()) {
					executeUpdate(pstmt, null, batchCount == joinEntities.size());
				}
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	private void updateBatch(final Collection<? extends Entity> entities) {
		try {
			long batchCount = 0;
			PreparedStatement pstmt = null;

			for (Entity entity : entities) {
				if (pstmt == null) {
					pstmt = EntityUpdate.getUpdate(entity.getClass());
					if (LOG_SQL) {
						LOGGER.log(Level.INFO, pstmt.toString());
		    		}
				}

				EntityUpdate.putUpdateValues(pstmt, entity);
				pstmt.addBatch();

				if (++batchCount % ConnectionFactory.BATCH_SIZE == 0 || batchCount == entities.size()) {
					executeUpdate(pstmt, null, batchCount == entities.size());
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ void delete(final Collection<? extends Entity> entities, boolean blockCascade) {
		if (entities != null && !entities.isEmpty()) {
			EntityContext.setBlockCascade(blockCascade);
			if (EntityHandler.isJoinTable(entities.iterator().next().getClass())) {
				deleteJoin(entities);
			} else {
				deleteDefault(entities);
			}
		} else {
			throw new RuntimeException("Collection of entities to be deleted cannot be null or empty!");
		}
		EntityContext.clearBuildBlockedClasses();
	}

	private void deleteDefault(final Collection<? extends Entity> entities) {

		final Map<Class<?>, List<Entity>> oneToOneMap = new HashMap<Class<?>, List<Entity>>();
		final Map<Class<?>, List<EntityBatch>> oneToManyMap = new HashMap<Class<?>, List<EntityBatch>>();
		final Map<Class<?>, List<EntityBatch>> manyToManyMap = new HashMap<Class<?>, List<EntityBatch>>();
		final Map<Class<?>, List<EntityBatch>> oneToManyToManyMap = new HashMap<Class<?>, List<EntityBatch>>();

		// Validate entity and collect update relations
		for (Entity entity : entities) {
			EntityHandler.validateColumns(entity);

        	// Get all references to OneToOne from all entities
        	EntityHandler.getOneToOneMap(entity, CascadeType.DELETE, oneToOneMap);

        	// Get all references to OneToMany from all entities
        	EntityHandler.getOneToManyMap(entity, CascadeType.DELETE, false, oneToManyMap);

        	// Get all references to ManyToMany from all entities
        	EntityHandler.getManyToManyMap(entity, CascadeType.DELETE, manyToManyMap);

            // Get all references to OneToMany related to ManyToMany from all entities
        	EntityHandler.getOneToManyMap(entity, CascadeType.DELETE, true, oneToManyToManyMap);
		}

		// Cascade for OneToMany references
		for (Class<?> clazz : oneToManyMap.keySet()) {
			for (EntityBatch entityBatch : oneToManyMap.get(clazz)) {
				Collection<Entity> entityCollection = entityBatch.getCollection();
				if (!entityCollection.isEmpty()) {
					deleteDefault(entityCollection);
				}
			}
		}

		// Cascade for ManyToMany references
		for (Class<?> clazz : manyToManyMap.keySet()) {
			for (EntityBatch entityBatch : manyToManyMap.get(clazz)) {
				Collection<Entity> entityCollection = entityBatch.getCollection();

				if (!entityCollection.isEmpty()) {
					deleteJoin(entityBatch.getManyToMany(), entityBatch.getEntity(), entityCollection);
	
					deleteDefault(entityCollection);
				}
			}
		}

		// Cascade for OneToMany references related to ManyToMany
		for (Class<?> clazz : oneToManyToManyMap.keySet()) {
			for (EntityBatch entityBatch : oneToManyToManyMap.get(clazz)) {
				Collection<Entity> entityCollection = entityBatch.getCollection();

				if (!entityCollection.isEmpty()) {
					EntityContext.addBuildBlockedClass(entityBatch.getEntity().getClass());
					deleteJoin(entityCollection);
	
					List<Entity> joinedEntityList = new ArrayList<Entity>(); 
					for (Entity ent : entityCollection) {
						joinedEntityList.addAll(EntityHandler.getJoinedTos(ent, entityBatch.getEntity().getClass()));
					}
					deleteDefault(joinedEntityList);
				}
			}
		}

		// Insert current references
		deleteBatch(entities);

		// Cascade for OneToOne references
		for (Class<?> clazz : oneToOneMap.keySet()) {
			deleteDefault(oneToOneMap.get(clazz));
		}
	}

	private void deleteJoin(final Collection<? extends Entity> joinEntities) {
		try {
			long batchCount = 0;
			PreparedStatement pstmt = null;

			for (Entity joinEntity : joinEntities) {
				EntityHandler.validateColumns(joinEntity);

				if (pstmt == null) {
					pstmt = EntityUpdate.getDeleteJoin(joinEntity);
					if (LOG_SQL) {
						LOGGER.log(Level.INFO, pstmt.toString());
					}
				}

				EntityUpdate.putDeleteJoinValues(pstmt, joinEntity);
				pstmt.addBatch();

				if (++batchCount % ConnectionFactory.BATCH_SIZE == 0 || batchCount == joinEntities.size()) {
					executeUpdate(pstmt, null, batchCount == joinEntities.size());
				}
			}

			final Map<Class<?>, List<Entity>> joinIdMap = new HashMap<Class<?>, List<Entity>>();

			// Validate entity and collect JoinId relations
			for (Entity joinEntity : joinEntities) {
				EntityHandler.validateColumns(joinEntity);

	        	// Get all references to JoinId from all entities
	        	EntityHandler.getJoinIdMap(joinEntity, CascadeType.DELETE, joinIdMap);
			}

			// Cascade for JoinId references
			for (Class<?> clazz : joinIdMap.keySet()) {
				List<Entity> joinIdList = joinIdMap.get(clazz);

				if (!joinIdList.isEmpty() && !EntityContext.containsBuildBlockedClass(clazz)) {
					deleteDefault(joinIdList);
				}
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	private void deleteJoin(final ManyToMany manyToMany, final Entity entityOne, final Collection<? extends Entity> entitiesTwo) {
		try {
			long batchCount = 0;
			PreparedStatement pstmt = null;

			for (Entity entityTwo : entitiesTwo) {
				if (pstmt == null) {
					pstmt = EntityUpdate.getDeleteJoin(entityOne.getClass(), entityTwo.getClass());
					if (LOG_SQL) {
						LOGGER.log(Level.INFO, pstmt.toString());
		    		}
				}

				EntityUpdate.putDeleteJoinValues(pstmt, manyToMany, entityOne, entityTwo);
				pstmt.addBatch();

				if (++batchCount % ConnectionFactory.BATCH_SIZE == 0 || batchCount == entitiesTwo.size()) {
					executeUpdate(pstmt, null, batchCount == entitiesTwo.size());
				}
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	private void deleteBatch(final Collection<? extends Entity> entities) {
		try {
			long batchCount = 0;
			PreparedStatement pstmt = null;

			for (Entity entity : entities) {
				if (pstmt == null) {
					pstmt = EntityUpdate.getDelete(entity);
					if (LOG_SQL) {
						LOGGER.log(Level.INFO, pstmt.toString());
		    		}
				}

				EntityUpdate.putDeleteValues(pstmt, entity);
				pstmt.addBatch();

				if (++batchCount % ConnectionFactory.BATCH_SIZE == 0 || batchCount == entities.size()) {
					executeUpdate(pstmt, null, batchCount == entities.size());
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	private List<Object> executeUpdate(final PreparedStatement pstmt, final Class<?> generatedIdClass, final boolean closePstmt) {
		final List<Object> rows = new ArrayList<Object>();
		if (pstmt != null) {
	        try {   
	            pstmt.executeBatch();
	            if (generatedIdClass != null) {
	                ResultSet rs = pstmt.getGeneratedKeys();

	                while (rs.next()) {
	                	rows.add(EntityResultSet.getResultSetValue(rs, GENERATED_KEY_INDEX, generatedIdClass));
	                }
	            }
	        } catch (SQLException ex) {
	        	throw new RuntimeException(ex);

	        } finally {
	        	if (closePstmt) {
		        	try {
		        		pstmt.close();
		        	} catch (SQLException ex) {
		        		throw new RuntimeException(ex);
		        	}
	        	}
	        }
        }
        return rows;
	}

}
