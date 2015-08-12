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

import java.util.Collection;
import java.util.List;

import com.jsmartdb.framework.types.OrderType;

@SuppressWarnings("unchecked")
public interface EntityManager {

	public void initTransaction();

	public void commitTransaction();

	public void rollbackTransaction();

	public void insertEntity(Entity entity);

	public void insertEntity(Entity entity, boolean blockCascade);

	public void insertBatchEntity(Collection<? extends Entity> entities);

	public void insertBatchEntity(Collection<? extends Entity> entities, boolean blockCascade);

	public void updateEntity(Entity entity);

	public void updateEntity(Entity entity, boolean blockCascade);

	public void updateBatchEntity(Collection<? extends Entity> entities);

	public void updateBatchEntity(Collection<? extends Entity> entities, boolean blockCascade);
 
	public void deleteEntity(Entity entity);

	public void deleteEntity(Entity entity, boolean blockCascade);

	public void deleteBatchEntity(Collection<? extends Entity> entities);

	public void deleteBatchEntity(Collection<? extends Entity> entities, boolean blockCascade);

	public Entity selectSingleEntity(Entity entity);

	public Entity selectSingleEntity(Entity entity, boolean blockJoin);

	public Entity selectSingleEntity(Entity entity, Class<? extends Entity> ... blockJoinClasses);

	public Entity selectSingleEntity(Entity entity, String orderBy, OrderType orderDir);

	public Entity selectSingleEntity(Entity entity, String orderBy, OrderType orderDir, boolean blockJoin);

	public Entity selectSingleEntity(Entity entity, String orderBy, OrderType orderDir, Class<? extends Entity> ... blockJoinClasses);

	public List<? extends Entity> selectEntity(Entity entity);

	public List<? extends Entity> selectEntity(Entity entity, boolean blockJoin);

	public List<? extends Entity> selectEntity(Entity entity, Class<? extends Entity> ... blockJoinClasses);

	public List<? extends Entity> selectEntity(Entity entity, String orderBy, OrderType orderDir);

	public List<? extends Entity> selectEntity(Entity entity, String orderBy, OrderType orderDir, boolean blockJoin);

	public List<? extends Entity> selectEntity(Entity entity, String orderBy, OrderType orderDir, Class<? extends Entity> ... blockJoinClasses);

	public List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max);

	public List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, boolean blockJoin);

	public List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, Class<? extends Entity> ... blockJoinClasses);

	public List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, String orderBy, OrderType orderDir);

	public List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, String orderBy, OrderType orderDir, boolean blockJoin);

	public List<? extends Entity> selectEntity(Entity entity, Integer first, Integer max, String orderBy, OrderType orderDir, Class<? extends Entity> ... blockJoinClasses);

	public Entity selectSingleEntity(QueryParam param);

	public Entity selectSingleEntity(QueryParam param, boolean blockJoin);

	public Entity selectSingleEntity(QueryParam param, Class<? extends Entity> ... blockJoinClasses);

	public Entity selectSingleEntity(QueryParam param, String orderBy, OrderType orderDir);

	public Entity selectSingleEntity(QueryParam param, String orderBy, OrderType orderDir, boolean blockJoin);

	public Entity selectSingleEntity(QueryParam param, String orderBy, OrderType orderDir, Class<? extends Entity> ... blockJoinClasses);

	public List<? extends Entity> selectEntity(QueryParam param);

	public List<? extends Entity> selectEntity(QueryParam param, boolean blockJoin);

	public List<? extends Entity> selectEntity(QueryParam param, Class<? extends Entity> ... blockJoinClasses);

	public List<? extends Entity> selectEntity(QueryParam param, String orderBy, OrderType orderDir);

	public List<? extends Entity> selectEntity(QueryParam param, String orderBy, OrderType orderDir, boolean blockJoin);

	public List<? extends Entity> selectEntity(QueryParam param, String orderBy, OrderType orderDir, Class<? extends Entity> ... blockJoinClasses);

	public List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max);

	public List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, boolean blockJoin);

	public List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, Class<? extends Entity> ... blockJoinClasses);

	public List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, String orderBy, OrderType orderDir);

	public List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, String orderBy, OrderType orderDir, boolean blockJoin);

	public List<? extends Entity> selectEntity(QueryParam param, Integer first, Integer max, String orderBy, OrderType orderDir, Class<? extends Entity> ... blockJoinClasses);

	public Object selectSingleObject(QueryParam param);

	public Object[] selectSingleArray(QueryParam param);

	public List<Object[]> selectObject(QueryParam param);

	public List<Object[]> selectObject(QueryParam param, Integer first, Integer max);

	public List<Object[]> executeNativeQuery(String query);

	public List<Object[]> executeNativeQuery(String query, Object[] params);

}