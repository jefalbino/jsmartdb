/*
 * JSmartDB - Java ORM Framework
 * Copyright (c) 2014, Jeferson Albino da Silva, All rights reserved.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
*/

package com.jsmartdb.framework.manager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.jsmartdb.framework.types.CascadeType;
import com.jsmartdb.framework.types.OrderType;

import com.jsmartdb.framework.annotation.ManyToMany;
import com.jsmartdb.framework.annotation.QueryFilter;

/*package*/ final class EntityRepository {

	private static Logger LOGGER = Logger.getLogger(EntityRepository.class.getPackage().getName());

	private static Boolean LOG_SQL = new Boolean(EntityPersistence.getInstance().getProperty(EntityPersistence.SQL_SHOW_SQL));

	private static final int EXECUTION_SUCCESS = 1;

	private static final int GENERATED_KEY_INDEX = 1;

	/*package*/ void insert(final Entity entity, boolean blockCascade) {
		if (entity != null) {
			EntityContext.setBlockCascade(blockCascade);
			if (EntityHandler.isJoinTable(entity.getClass())) {
				insertJoin(entity);
			} else {
				insertDefault(entity);
			}
		} else {
			throw new RuntimeException("Entity to be inserted cannot be null!");
		}
		EntityContext.clearBuildBlockedClasses();
	}

	private void insertDefault(final Entity entity) {
        if (entity != null) {

        	EntityHandler.validateColumns(entity);

        	// Cascade for OneToOne references
            final List<Entity> oneToOneList = EntityHandler.getOneToOneList(entity, CascadeType.INSERT);
            for (Entity ent : oneToOneList) {
            	if (ent != null) { 
            		insertDefault(ent);
            	}
            }

            final PreparedStatement pstmt = EntityUpdate.getInsert(entity.getClass());
            EntityUpdate.putInsertValues(pstmt, entity);

            Class<?> generatedIdClass = EntityHandler.getGeneratedIdClass(entity);
            Object generatedValue = executeUpdate(pstmt, generatedIdClass);
            if (generatedIdClass != null) {
            	EntityHandler.setGeneratedValue(entity, generatedValue);
            }

            EntityHandler.setInternalId(entity);

        	// Cascade for OneToMany references
            List<Collection<Entity>> oneToManyList = EntityHandler.getOneToManyList(entity, CascadeType.INSERT, false);
            for (Collection<Entity> collection : oneToManyList) {
            	for (Entity ent : collection) {
            		if (ent != null) {
            			insertDefault(ent);
            		}
            	}
            }

            // Cascade for ManyToMany references
            final List<EntityManyToMany> manyToManyList = EntityHandler.getManyToManyList(entity, CascadeType.INSERT);
            for (EntityManyToMany entityManyToMany : manyToManyList) {
            	for (Entity ent : entityManyToMany.getCollection()) {
            		if (ent != null) {
            			if (EntityContext.isManyToManyCascade(entity.getClass(), ent.getClass())) {
            				insertDefault(ent);
            			}
            			insertJoin(entityManyToMany.getManyToMany(), entity, ent);
            		}
            	}
            }

            // Cascade for OneToMany references related to ManyToMany
            oneToManyList = EntityHandler.getOneToManyList(entity, CascadeType.INSERT, true);
            for (Collection<Entity> collection : oneToManyList) {
            	for (Entity ent : collection) {
            		if (ent != null) {

	            		if (EntityContext.isManyToManyCascade(entity.getClass(), ent.getClass())) {
	            			List<Entity> joinedEntityList = EntityHandler.getJoinedTos(ent, entity.getClass());

	            			for (Entity joinedEntity : joinedEntityList) {
	            				EntityContext.addBuildBlockedClass(joinedEntity.getClass());
	            				insertDefault(joinedEntity);
	            			}
	            		}

	            		EntityContext.addBuildBlockedClass(entity.getClass());
	            		insertJoin(ent);
            		}
            	}
            }
        }
    }

	private void insertJoin(final Entity joinEntity) {
		if (joinEntity != null) {

			EntityHandler.validateColumns(joinEntity);

			// Cascade for JoinId references
            final List<Entity> joinIdList = EntityHandler.getJoinIdList(joinEntity, CascadeType.INSERT);
            for (Entity ent : joinIdList) {
        		if (ent != null && !EntityContext.containsBuildBlockedClass(ent.getClass())) {
        			insertDefault(ent);
        		}
            }

			final PreparedStatement pstmt = EntityUpdate.getInsertJoin(joinEntity.getClass());
			EntityUpdate.putInsertJoinValues(pstmt, joinEntity);
			executeUpdate(pstmt, null).equals(EXECUTION_SUCCESS);
			EntityHandler.setInternalId(joinEntity);
		}
	}

	private void insertJoin(final ManyToMany manyToMany, final Entity entityOne, final Entity entityTwo) {
		if (entityOne != null && entityTwo != null) {
			final PreparedStatement pstmt = EntityUpdate.getInsertJoin(entityOne.getClass(), entityTwo.getClass());
			EntityUpdate.putInsertJoinValues(pstmt, manyToMany, entityOne, entityTwo);
			executeUpdate(pstmt, null).equals(EXECUTION_SUCCESS);
		}
	}

	/*package*/ void update(final Entity entity, boolean blockCascade) {
		if (entity != null) {
			EntityContext.setBlockCascade(blockCascade);
			if (EntityHandler.isJoinTable(entity.getClass())) {
				updateJoin(entity);
			} else {
				updateDefault(entity);
			}
		} else {
			throw new RuntimeException("Entity to be updated cannot be null!");
		}
		EntityContext.clearBuildBlockedClasses();
	}

	private void updateDefault(final Entity entity) {
        if (entity != null) {

        	EntityHandler.validateColumns(entity);

        	// Cascade for OneToOne references
            final List<Entity> oneToOneList = EntityHandler.getOneToOneList(entity, CascadeType.UPDATE);
            for (Entity ent : oneToOneList) {
            	if (ent != null) {
	                if (ent.internalId == null) {
	                    insertDefault(ent);
	                } else {
	                    updateDefault(ent);
	                }
            	}
            }

            final PreparedStatement pstmt = EntityUpdate.getUpdate(entity.getClass());
            EntityUpdate.putUpdateValues(pstmt, entity);
            executeUpdate(pstmt, null).equals(EXECUTION_SUCCESS);

        	// Cascade for OneToMany references
        	List<Collection<Entity>> oneToManyList = EntityHandler.getOneToManyList(entity, CascadeType.UPDATE, false);
            for (Collection<Entity> collection : oneToManyList) {
            	for (Entity ent : collection) {
            		if (ent != null) {
	            		if (ent.internalId == null) {
	            			insertDefault(ent);
	            		} else {
	            			updateDefault(ent);
	            		}
            		}
            	}
            }

            // Cascade for ManyToMany references
            final List<EntityManyToMany> manyToManyList = EntityHandler.getManyToManyList(entity, CascadeType.UPDATE);
            for (EntityManyToMany entityManyToMany : manyToManyList) {
                for (Entity ent : entityManyToMany.getCollection()) {
                	if (ent != null) {
                        if (EntityContext.isManyToManyCascade(entity.getClass(), ent.getClass())) {
                            if (ent.internalId == null) {
                                insertDefault(ent);
                            } else {
                                updateDefault(ent);
                            }
                        }
                	}
                }
            }

            // Cascade for OneToMany references related to ManyToMany
            oneToManyList = EntityHandler.getOneToManyList(entity, CascadeType.UPDATE, true);
            for (Collection<Entity> collection : oneToManyList) {
            	for (Entity ent : collection) {
            		if (ent != null) {

	            		if (EntityContext.isManyToManyCascade(entity.getClass(), ent.getClass())) {
	            			List<Entity> joinedEntityList = EntityHandler.getJoinedTos(ent, entity.getClass());

	            			for (Entity joinedEntity : joinedEntityList) {
	            				if (joinedEntity.internalId == null) {
		            				insertDefault(joinedEntity);
		            			} else {
		            				updateDefault(joinedEntity);
		            			}
	            			}
                        }
	            		EntityContext.addBuildBlockedClass(entity.getClass());
	            		updateJoin(ent);
            		}
            	}
            }
        }
    }

	private void updateJoin(final Entity joinEntity) {
		if (joinEntity != null) {

			EntityHandler.validateColumns(joinEntity);

			// Cascade for JoinId references
            final List<Entity> joinIdList = EntityHandler.getJoinIdList(joinEntity, CascadeType.UPDATE);
            for (Entity ent : joinIdList) {
            	if (ent != null && !EntityContext.containsBuildBlockedClass(ent.getClass())) {
	                if (ent.internalId == null) {
	                    insertDefault(ent);
	                } else {
	                    updateDefault(ent);
	                }
            	}
            }

			final PreparedStatement pstmt = EntityUpdate.getUpdateJoin(joinEntity.getClass());
			EntityUpdate.putUpdateJoinValues(pstmt, joinEntity);
			executeUpdate(pstmt, null).equals(EXECUTION_SUCCESS);
		}
	}

	/*package*/ void delete(final Entity entity, boolean blockCascade) {
		if (entity != null) {
			EntityContext.setBlockCascade(blockCascade);
			if (EntityHandler.isJoinTable(entity.getClass())) {
				deleteJoin(entity);
			} else {
				deleteDefault(entity);
			}
		} else {
			throw new RuntimeException("Entity to be deleted cannot be null!");
		}
		EntityContext.clearBuildBlockedClasses();
	}

	private void deleteDefault(final Entity entity) {
        if (entity != null) {
        	
        	EntityHandler.validateColumns(entity);

        	// Cascade for OneToMany references
        	List<Collection<Entity>> oneToManyList = EntityHandler.getOneToManyList(entity, CascadeType.DELETE, false);
            for (Collection<Entity> collection : oneToManyList) {
            	for (Entity ent : collection) {
            		if (ent != null) {
            			deleteDefault(ent);
            		}
            	}
            }

            // Cascade for ManyToMany references
            final List<EntityManyToMany> manyToManyList = EntityHandler.getManyToManyList(entity, CascadeType.DELETE);
            for (EntityManyToMany entityManyToMany : manyToManyList) {
            	for (Entity ent : entityManyToMany.getCollection()) {
            		if (ent != null) {
	            		deleteJoin(entityManyToMany.getManyToMany(), entity, ent);
	            		if (EntityContext.isManyToManyCascade(entity.getClass(), ent.getClass())) {
	            			deleteDefault(ent);
	            		}
            		}
            	}
            }

            // Cascade for OneToMany references related to ManyToMany
            oneToManyList = EntityHandler.getOneToManyList(entity, CascadeType.DELETE, true);
            for (Collection<Entity> collection : oneToManyList) {
            	for (Entity ent : collection) {
            		if (ent != null) {
            			EntityContext.addBuildBlockedClass(entity.getClass());
	            		deleteJoin(ent);

	            		if (EntityContext.isManyToManyCascade(entity.getClass(), ent.getClass())) {
	            			List<Entity> joinedEntityList = EntityHandler.getJoinedTos(ent, entity.getClass());

	            			for (Entity joinedEntity : joinedEntityList) {
	            				deleteDefault(joinedEntity);
	            			}
	            		}
            		}
            	}
            }

            // Delete current object
            final PreparedStatement pstmt = EntityUpdate.getDelete(entity);
            EntityUpdate.putDeleteValues(pstmt, entity);
            executeUpdate(pstmt, null).equals(EXECUTION_SUCCESS);

            // Cascade for OneToOne references
            final List<Entity> oneToOneList = EntityHandler.getOneToOneList(entity, CascadeType.DELETE);
            for (Entity ent : oneToOneList) {
            	if (ent != null) {
            		deleteDefault(ent);
            	}
            }
        }
    }

	private void deleteJoin(final Entity joinEntity) {
		if (joinEntity != null) {

			EntityHandler.validateColumns(joinEntity);

			final PreparedStatement pstmt = EntityUpdate.getDeleteJoin(joinEntity);
			EntityUpdate.putDeleteJoinValues(pstmt, joinEntity);
			executeUpdate(pstmt, null).equals(EXECUTION_SUCCESS);

			// Cascade for JoinId references
            final List<Entity> joinIdList = EntityHandler.getJoinIdList(joinEntity, CascadeType.DELETE);
            for (Entity ent : joinIdList) {
            	if (ent != null && !EntityContext.containsBuildBlockedClass(ent.getClass())) {
            		deleteDefault(ent);
            	}
            }
		}
	}

	private void deleteJoin(final ManyToMany manyToMany, final Entity entityOne, final Entity entityTwo) {
		if (entityOne != null && entityTwo != null) {
			final PreparedStatement pstmt = EntityUpdate.getDeleteJoin(entityOne.getClass(), entityTwo.getClass());
			EntityUpdate.putDeleteJoinValues(pstmt, manyToMany, entityOne, entityTwo);
			executeUpdate(pstmt, null).equals(EXECUTION_SUCCESS);
		}
	}

	@SuppressWarnings("unchecked")
	/*package*/ List<? extends Entity> find(final Entity entity, final Integer first, final Integer max, 
			final String orderBy, final OrderType orderDir, final boolean blockJoin, final Class<? extends Entity> ... blockJoinClasses) {

		if (entity == null) {
			throw new RuntimeException("Entity to be selected cannot be null!");
		}

		validateFindParameters(first, max, orderBy, blockJoinClasses);

		EntityContext.setFirstResult(first);
		EntityContext.setMaxResult(max);
		EntityContext.setOrderBy(orderBy);
		EntityContext.setOrderDir(orderDir);
		EntityContext.setBlockJoin(blockJoin);
		EntityContext.addJoinBLockClasses(blockJoinClasses);

		final PreparedStatement pstmt = EntitySelect.getSelectQuery(entity);

		return executeQuery(pstmt, entity.getClass());
	}

    @SuppressWarnings("unchecked")
    /*package*/ List<? extends Object> findBy(final QueryParam param, final Integer first, final Integer max, 
    		final String orderBy, final OrderType orderDir, final boolean blockJoin, final Class<? extends Entity> ... blockJoinClasses) {

    	if (param == null || !param.isValid()) {
    		throw new RuntimeException("QueryParam is not valid!");
    	}

    	validateFindParameters(first, max, orderBy, blockJoinClasses);

		EntityContext.setFirstResult(first);
		EntityContext.setMaxResult(max);
		EntityContext.setOrderBy(orderBy);
		EntityContext.setOrderDir(orderDir);
		EntityContext.setBlockJoin(blockJoin);
		EntityContext.addJoinBLockClasses(blockJoinClasses);

    	final QueryFilter filter = EntityHandler.getQueryFilter(param);

    	final PreparedStatement pstmt = EntitySelect.getCustomSelectQuery(param.getEntityClazz(), filter, param);

        return filter.select().trim().isEmpty() ? executeQuery(pstmt, param.getEntityClazz()) : executeQuery(pstmt);
    }

    @SuppressWarnings("unchecked")
    private void validateFindParameters(final Integer first, final Integer max, final String orderBy, final Class<? extends Entity> ... blockJoinClasses) {

    	if (orderBy != null && orderBy.trim().isEmpty()) {
			throw new RuntimeException("QueryParam contains invalid orderBy parameter!");
		}

		if (first != null && first < 0) {
			throw new RuntimeException("First must be greater or equal to zero!");
		}

		if (max != null && max < 0) {
			throw new RuntimeException("Max must be greater or equal to zero!");
		}

		if (blockJoinClasses != null && blockJoinClasses.length == 0) {
			throw new RuntimeException("blockJoinClasses cannot be empty!");
		}
    }

    /*package*/ List<? extends Object> nativeQuery(final String query, final Object[] params) {
    	final List<Object[]> objects = new ArrayList<Object[]>();
    	
    	if (query == null || query.trim().isEmpty()) {
    		throw new RuntimeException("Query to be executed cannot be null or empty!");
    	}

    	if (query.contains("?") && (params == null || params.length == 0)) {
    		throw new RuntimeException("Params array cannot be null or empty for queries with parameters!");
    	}

    	if (params != null && params.length > 0 && !query.contains("?")) {
    		throw new RuntimeException("Params array must be null or empty for queries without parameters!");
    	}

		final PreparedStatement pstmt = EntityUpdate.getNative(query);
		EntityUpdate.putNativeValues(pstmt, params);

		if (query.toLowerCase().startsWith(EntitySelect.SELECT_STATEMENT)) {
			objects.addAll(executeQuery(pstmt));
		} else {
			objects.add(new Object[]{executeUpdate(pstmt, null)});
		}
    	return objects;
    }

    private Object executeUpdate(final PreparedStatement pstmt, Class<?> generatedIdClass) {
    	Object row = 0;
        if (pstmt != null) {
        	try {
	        	if (LOG_SQL) {
	    			LOGGER.log(Level.INFO, pstmt.toString());
	    		}

	            row = pstmt.executeUpdate();

	            if (generatedIdClass != null) {
	                ResultSet rs = pstmt.getGeneratedKeys();

	                if (rs.next()) {
	                	row = EntityResultSet.getResultSetValue(rs, GENERATED_KEY_INDEX, generatedIdClass);
	                }
	            }
	        } catch (SQLException ex) {
	        	throw new RuntimeException(ex);

	        } finally {
	        	try {
	        		pstmt.close();
	        	} catch (SQLException ex) {
	        		throw new RuntimeException(ex);
	        	}
	        }
        }
        return row;
    }

    private List<Object[]> executeQuery(final PreparedStatement pstmt) {
    	final List<Object[]> objects = new ArrayList<Object[]>();
    	if (pstmt != null) {
            try {
            	if (LOG_SQL) {
        			LOGGER.log(Level.INFO, pstmt.toString());
        		}

                ResultSet rs = pstmt.executeQuery();

                while (rs.next()) {
                	Object[] object = new Object[rs.getMetaData().getColumnCount()];

                	for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                		object[i] = rs.getObject(i + 1, SQLTypes.getSQLTypes());
                	}
                	objects.add(object);
                }
            } catch (SQLException ex) {
            	throw new RuntimeException(ex);

            } finally {
            	try {
            		pstmt.close();
            	} catch (SQLException ex) {
            		throw new RuntimeException(ex);
            	}
            }
        }
        return objects;
    }

    private List<? extends Entity> executeQuery(final PreparedStatement pstmt, final Class<? extends Entity> entityClazz) {
        final List<Entity> entities = new ArrayList<Entity>();

        if (pstmt != null) {
            try {
            	if (LOG_SQL) {
        			LOGGER.log(Level.INFO, pstmt.toString());
        		}

                final ResultSet rs = pstmt.executeQuery();

                String alias = null;
                Entity lastEntity = null;

                // While to manage result set rows and joins
                while (rs.next()) {
                	EntityContext.clearBuildBlockedClasses();

                	if (lastEntity != null) {
                		alias = EntityAlias.getAlias(lastEntity.getClass());
                	}

                	if (EntityResultSet.reuseEntity(lastEntity, rs, alias)) {
                		EntityResultSet.setJoinResultSet(lastEntity, rs);
                	} else {
                		lastEntity = EntityResultSet.createEntity(entityClazz, rs);
                		entities.add(lastEntity);
                	}
                }
            } catch (Exception ex) {
            	throw new RuntimeException(ex);

            } finally {
            	try {
	            	pstmt.close();
            	} catch (SQLException ex) {
            		throw new RuntimeException(ex);
            	}
            }
        }
        return entities;
    }

}
