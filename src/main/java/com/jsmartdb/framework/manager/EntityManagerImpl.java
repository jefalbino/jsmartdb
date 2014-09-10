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

import java.util.Collection;
import java.util.List;

import com.jsmartdb.framework.types.OrderType;

@SuppressWarnings("unchecked")
/*package*/ final class EntityManagerImpl implements EntityManager {

	private static final Integer FIRST = 1;

	@Override
	public final void initTransaction() {
		EntityContext.initUserTransaction();
	}

	@Override
	public final void commitTransaction() {
		EntityContext.closeUserTransaction(false);
	}

	@Override
	public final void rollbackTransaction() {
		EntityContext.closeUserTransaction(true);
	}

    @Override
    public final void insertEntity(Entity entity) {
    	insertEntity(entity, false);
    }
   
    @Override
    public final void insertEntity(Entity entity, boolean blockCascade) {
    	EntityRepository repository = new EntityRepository();
        repository.insert(entity, blockCascade);
    }

	@Override
	public final void insertBatchEntity(Collection<? extends Entity> entities) {
		insertBatchEntity(entities, false);
	}

	@Override
	public final void insertBatchEntity(Collection<? extends Entity> entities, boolean blockCascade) {
		EntityBatchRepository batchRepository = new EntityBatchRepository();
		batchRepository.insert(entities, blockCascade);
	}

    @Override
    public final void updateEntity(Entity entity) {
    	updateEntity(entity, false);
    }

    @Override
    public final void updateEntity(Entity entity, boolean blockCascade) {
        EntityRepository repository = new EntityRepository();
        repository.update(entity, blockCascade);
    }

	@Override
	public final void updateBatchEntity(Collection<? extends Entity> entities) {
		updateBatchEntity(entities, false);
	}

	@Override
	public final void updateBatchEntity(Collection<? extends Entity> entities, boolean blockCascade) {
		EntityBatchRepository batchRepository = new EntityBatchRepository();
		batchRepository.update(entities, blockCascade);
	}

    @Override
    public final void deleteEntity(Entity entity) {
    	deleteEntity(entity, false);
    }

    @Override
    public final void deleteEntity(Entity entity, boolean blockCascade) {
        EntityRepository repository = new EntityRepository();
        repository.delete(entity, blockCascade);
    }

	@Override
	public final void deleteBatchEntity(Collection<? extends Entity> entities) {
		deleteBatchEntity(entities, false);
	}

	@Override
	public final void deleteBatchEntity(Collection<? extends Entity> entities, boolean blockCascade) {
		EntityBatchRepository batchRepository = new EntityBatchRepository();
		batchRepository.delete(entities, blockCascade);
	}

	@Override
	public final Entity selectSingleEntity(Entity entity) {
		return selectSingleEntity(entity, false);
	}

	@Override
	public final Entity selectSingleEntity(Entity entity, boolean blockJoin) {
		List<Entity> entities = (List<Entity>) selectEntity(entity, 0, FIRST, blockJoin);
		return !entities.isEmpty() ? entities.get(0) : null;
	}

	@Override
	public final Entity selectSingleEntity(Entity entity, Class<? extends Entity>... blockJoinClasses) {
		return selectSingleEntity(entity, null, null, blockJoinClasses);
	}

	@Override
	public final Entity selectSingleEntity(Entity entity, String orderBy, OrderType orderDir) {
		return selectSingleEntity(entity, orderBy, orderDir, false);
	}

	@Override
	public Entity selectSingleEntity(Entity entity, String orderBy, OrderType orderDir, boolean blockJoin) {
		List<Entity> entities = (List<Entity>) selectEntity(entity, 0, FIRST, orderBy, orderDir, blockJoin);
		return !entities.isEmpty() ? entities.get(0) : null;
	}

	@Override
	public Entity selectSingleEntity(Entity entity, String orderBy,	OrderType orderDir, Class<? extends Entity>... blockJoinClasses) {
		List<Entity> entities = (List<Entity>) selectEntity(entity, 0, FIRST, orderBy, orderDir, blockJoinClasses);
		return !entities.isEmpty() ? entities.get(0) : null;
	}

    @Override
    public final List<? extends Entity> selectEntity(Entity entity) {
    	return selectEntity(entity, 0, Integer.MAX_VALUE);
    }
   
    @Override
	public final List<? extends Entity> selectEntity(Entity entity, boolean blockJoin) {
		return selectEntity(entity, 0, Integer.MAX_VALUE, blockJoin);
	}

	@Override
	public final List<? extends Entity> selectEntity(Entity entity, Class<? extends Entity>... blockJoinClasses) {
		return selectEntity(entity, 0, Integer.MAX_VALUE, blockJoinClasses);
	}

	@Override
	public final List<? extends Entity> selectEntity(Entity entity, String orderBy, OrderType orderDir) {
		return selectEntity(entity, 0, Integer.MAX_VALUE, orderBy, orderDir);
	}

	@Override
	public List<? extends Entity> selectEntity(Entity entity, String orderBy, OrderType orderDir, boolean blockJoin) {
		return selectEntity(entity, 0, Integer.MAX_VALUE, orderBy, orderDir, blockJoin);
	}

	@Override
	public List<? extends Entity> selectEntity(Entity entity, String orderBy, OrderType orderDir, Class<? extends Entity>... blockJoinClasses) {
		return selectEntity(entity, 0, Integer.MAX_VALUE, orderBy, orderDir, blockJoinClasses);
	}

	@Override
	public final List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max) {
        return selectEntity(entity, first, max, null, null);
	}

	@Override
	public final List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, boolean blockJoin) {
		return selectEntity(entity, first, max, null, null, blockJoin);
	}

	@Override
	public final List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, Class<? extends Entity>... blockJoinClasses) {
		return selectEntity(entity, first, max, null, null, blockJoinClasses);
	}

	@Override
	public final List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, String orderBy, OrderType orderDir) {
		return selectEntity(entity, first, max, orderBy, orderDir, false);
	}

	@Override
	public final List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, String orderBy, OrderType orderDir, boolean blockJoin) {
		EntityRepository repository = new EntityRepository();
        return repository.find(entity, first, max, orderBy, orderDir, blockJoin, (Class<? extends Entity>) null);
	}

	@Override
	public final List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, String orderBy, OrderType orderDir, Class<? extends Entity>... blockJoinClasses) {
		EntityRepository repository = new EntityRepository();
        return repository.find(entity, first, max, orderBy, orderDir, false, blockJoinClasses);
	}


	@Override
	public final Entity selectSingleEntity(QueryParam param) {
		return selectSingleEntity(param, false);
	}

	@Override
	public final Entity selectSingleEntity(QueryParam param, boolean blockJoin) {
		List<Entity> entities = (List<Entity>) selectEntity(param, 0, FIRST, blockJoin);
		return !entities.isEmpty() ? entities.get(0) : null;
	}

	@Override
	public final Entity selectSingleEntity(QueryParam param, Class<? extends Entity>... blockJoinClasses) {
		return selectSingleEntity(param, null, null, blockJoinClasses);
	}

	@Override
	public final Entity selectSingleEntity(QueryParam param, String orderBy, OrderType orderDir) {
		return selectSingleEntity(param, orderBy, orderDir, false);
	}

	@Override
	public Entity selectSingleEntity(QueryParam param, String orderBy, OrderType orderDir, boolean blockJoin) {
		List<Entity> entities = (List<Entity>) selectEntity(param, 0, FIRST, orderBy, orderDir, blockJoin);
		return !entities.isEmpty() ? entities.get(0) : null;
	}

	@Override
	public Entity selectSingleEntity(QueryParam param, String orderBy, OrderType orderDir, Class<? extends Entity>... blockJoinClasses) {
		List<Entity> entities = (List<Entity>) selectEntity(param, 0, FIRST, orderBy, orderDir, blockJoinClasses);
		return !entities.isEmpty() ? entities.get(0) : null;
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param) {
		return selectEntity(param, false);
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, boolean blockJoin) {
		return selectEntity(param, 0, Integer.MAX_VALUE, blockJoin);
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, Class<? extends Entity>... blockJoinClasses) {
		return selectEntity(param, 0, Integer.MAX_VALUE, blockJoinClasses);
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max) {
        return selectEntity(param, first, max, false);
    }

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, boolean blockJoin) {
		return selectEntity(param, first, max, null, null, blockJoin);
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, Class<? extends Entity>... blockJoinClasses) {
		return selectEntity(param, first, max, null, null, blockJoinClasses);
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, String orderBy, OrderType orderDir) {
		return selectEntity(param, 0, Integer.MAX_VALUE, orderBy, orderDir);
	}

	@Override
	public List<? extends Entity> selectEntity(QueryParam param, String orderBy, OrderType orderDir, boolean blockJoin) {
		return selectEntity(param, 0, Integer.MAX_VALUE, orderBy, orderDir, blockJoin);
	}

	@Override
	public List<? extends Entity> selectEntity(QueryParam param, String orderBy, OrderType orderDir, Class<? extends Entity>... blockJoinClasses) {
		return selectEntity(param, 0, Integer.MAX_VALUE, orderBy, orderDir, blockJoinClasses);
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, String orderBy, OrderType orderDir) {
        return selectEntity(param, first, max, orderBy, orderDir, false);
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, String orderBy, OrderType orderDir, boolean blockJoin) {
		EntityRepository repository = new EntityRepository();
        return (List<Entity>) repository.findBy(param, first, max, orderBy, orderDir, blockJoin, (Class<? extends Entity>) null);
	}

	@Override
	public final List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, String orderBy, OrderType orderDir, Class<? extends Entity> ... blockJoinClasses) {
		EntityRepository repository = new EntityRepository();
        return (List<Entity>) repository.findBy(param, first, max, orderBy, orderDir, false, blockJoinClasses);
	}

	@Override
	public final Object selectSingleObject(QueryParam param) {
		Object[] objects = selectSingleArray(param);
		return objects != null && objects.length > 0 ? objects[0] : null;
	}

	@Override
	public final Object[] selectSingleArray(QueryParam param) {
		List<Object[]> objects = selectObject(param, 0, FIRST);
		return !objects.isEmpty() ? objects.get(0) : null;
	}

	@Override
	public final List<Object[]> selectObject(QueryParam param) {
		return selectObject(param, 0, Integer.MAX_VALUE);
	}

	@Override
	public final List<Object[]> selectObject(QueryParam param, Integer first, Integer max) {
		EntityRepository repository = new EntityRepository();
        return (List<Object[]>) repository.findBy(param, first, max, null, null, false, (Class<? extends Entity>) null);
	}

	@Override
	public final List<Object[]> executeNativeQuery(String query) {
		return executeNativeQuery(query, null);
	}

	@Override
	public final List<Object[]> executeNativeQuery(String query, Object[] params) {
		EntityRepository repository = new EntityRepository();
		return (List<Object[]>) repository.nativeQuery(query, params);
	}

}
