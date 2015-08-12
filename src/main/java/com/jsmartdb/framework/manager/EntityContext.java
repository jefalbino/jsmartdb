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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.jsmartdb.framework.types.OrderType;

/*package*/ final class EntityContext {

	/*package*/ static final Integer DEFAULT = -1;

	private static final Map<Thread, EntityContext> CONTEXT_MAP = new HashMap<Thread, EntityContext>();

	private Connection connection;

	private boolean rollbackChanges;

	private boolean userTransaction;


	private Map<String, String> aliases;

	private int aliasCounter;

	private Set<Class<?>> buildBlockedClasses;

	private Field buildBlockedField;

	private boolean buildJoinedField;

	private Set<String> manyToManyCascades;


	private String orderBy;

	private OrderType orderDir;

	private boolean blockCascade;

	private boolean blockJoin;

	private Set<Class<?>> joinBlockClasses;

	private Integer firstResult = DEFAULT;

	private Integer maxResult = DEFAULT;


	private StringBuilder selectBuilder;

	private StringBuilder joinBuilder;

	private StringBuilder whereBuilder;

	private StringBuilder orderBuilder;

	private List<Object> builderValues;

	private EntityContext() {
		clearBuilderContext();
		aliases = new HashMap<String, String>();
		orderBy = null;
		orderDir = null;
		firstResult = DEFAULT;
		maxResult = DEFAULT;
		blockJoin = false;
		blockCascade = false;
		joinBlockClasses = new HashSet<Class<?>>();
		connection = ConnectionFactory.getConnection();
	}

	private EntityContext(boolean userTransaction) {
		this();
		this.userTransaction = userTransaction; 
	}

	private final void close() {
		ConnectionFactory.putConnection(connection, rollbackChanges);
		connection = null;
		aliases = null;
		selectBuilder = null;
		joinBuilder = null;
		whereBuilder = null;
		orderBuilder = null;
		manyToManyCascades = null;
	}

	private final boolean isUserTransaction() {
		return userTransaction;
	}

	private final void clearBuilderContext() {
		selectBuilder = new StringBuilder(EntitySelect.SELECT_DISTINCT_STATEMENT);
		joinBuilder = new StringBuilder(EntitySelect.FROM_STATEMENT);
		whereBuilder = new StringBuilder(EntityWhere.WHERE_STATEMENT);
		orderBuilder = new StringBuilder(EntitySelect.ORDER_BY_STATEMENT);
		builderValues = new ArrayList<Object>();
		manyToManyCascades = new HashSet<String>();
		buildBlockedClasses = new HashSet<Class<?>>();
	}

	private static final EntityContext getCurrentInstance() {
		return CONTEXT_MAP.get(Thread.currentThread());
	}

	/*package*/ static final void initCurrentInstance() {
		if (!CONTEXT_MAP.containsKey(Thread.currentThread())) {
			CONTEXT_MAP.put(Thread.currentThread(), new EntityContext());
		}
	}

	/*package*/ static final void closeCurrentInstance() {
		final EntityContext context = CONTEXT_MAP.get(Thread.currentThread());
		if (context != null && !context.isUserTransaction()) {
			CONTEXT_MAP.remove(Thread.currentThread()).close();
		}
	}

	/*package*/ static final void initUserTransaction() {
		final EntityContext context = CONTEXT_MAP.get(Thread.currentThread());
		if (context != null) {
			CONTEXT_MAP.remove(Thread.currentThread()).close();
		}
		CONTEXT_MAP.put(Thread.currentThread(), new EntityContext(true));
	}

	/*package*/ static final void closeUserTransaction(boolean rollbackChanges) {
		final EntityContext context = CONTEXT_MAP.get(Thread.currentThread());
		if (context != null && context.isUserTransaction()) {
			CONTEXT_MAP.remove(Thread.currentThread());
			context.rollbackChanges = rollbackChanges;
			context.close();
		}
	}

	/*package*/ static final Connection getConnection() {
		return getCurrentInstance().connection;
	}

	/*package*/ static final void setConnection(Connection connection) {
		getCurrentInstance().connection = connection;
	}

	/*package*/ static final boolean isRollbackChanges() {
		return getCurrentInstance().rollbackChanges;
	}

	/*package*/ static final void setRollbackChanges(boolean rollbackChanges) {
		getCurrentInstance().rollbackChanges = rollbackChanges;
	}



	/*package*/ static final String getOrderBy() {
		return getCurrentInstance().orderBy;
	}

	/*package*/ static final void setOrderBy(String orderBy) {
		getCurrentInstance().orderBy = orderBy;
	}

	/*package*/ static final OrderType getOrderDir() {
		return getCurrentInstance().orderDir;
	}

	/*package*/ static final void setOrderDir(OrderType orderDir) {
		getCurrentInstance().orderDir = orderDir;
	}

	/*package*/ static final boolean isBlockCascade() {
		return getCurrentInstance().blockCascade;
	}

	/*package*/ static final void setBlockCascade(boolean blockCascade) {
		getCurrentInstance().blockCascade = blockCascade;
	}

	/*package*/ static final boolean isBlockJoin() {
		return getCurrentInstance().blockJoin;
	}

	/*package*/ static final void setBlockJoin(boolean blockJoin) {
		getCurrentInstance().blockJoin = blockJoin;
	}

	/*package*/ static final Set<Class<?>> getJoinBlockClasses() {
		return getCurrentInstance().joinBlockClasses;
	}

	/*package*/ static final void addJoinBLockClasses(Class<?> ... joinBlockClasses) {
		if (joinBlockClasses != null) {
			EntityContext context = getCurrentInstance();
			for (Class<?> joinBlockClass : joinBlockClasses) {
				context.joinBlockClasses.add(joinBlockClass);
			}
		}
	}

	/*package*/ static final void setJoinBlockClasses(Set<Class<?>> joinBlockClasses) {
		getCurrentInstance().joinBlockClasses.addAll(joinBlockClasses);
	}

	/*package*/ static final Integer getFirstResult() {
		return getCurrentInstance().firstResult;
	}

	/*package*/ static final void setFirstResult(Integer firstResult) {
		if (firstResult != null) {
			getCurrentInstance().firstResult = firstResult;
		}
	}

	/*package*/ static final Integer getMaxResult() {
		return getCurrentInstance().maxResult;
	}

	/*package*/ static final void setMaxResult(Integer maxResult) {
		if (maxResult != null) {
			getCurrentInstance().maxResult = maxResult;
		}
	}

	

	/*package*/ static final String getAliasCounter() {
		return (++getCurrentInstance().aliasCounter) + "_";
	}

	/*package*/ static final boolean containsBuildBlockedClass(Class<?> clazz) {
		return getCurrentInstance().buildBlockedClasses.contains(clazz);
	}

	/*package*/ static final void addBuildBlockedClass(Class<?> clazz) {
		getCurrentInstance().buildBlockedClasses.add(clazz);
	}

	/*package*/ static final void clearBuildBlockedClasses() {
		getCurrentInstance().buildBlockedClasses.clear();
	}

	/*package*/ static final Field getBuildBlockedField() {
		return getCurrentInstance().buildBlockedField;
	}

	/*package*/ static final void setBlockedField(Field blockedField) {
		getCurrentInstance().buildBlockedField = blockedField;
	}

	/*package*/ static final boolean isBuildJoinedField() {
		return getCurrentInstance().buildJoinedField;
	}

	/*package*/ static final void setBuildJoinedField(boolean joinedField) {
		getCurrentInstance().buildJoinedField = joinedField;
	}

	/*package*/ static final boolean isManyToManyCascade(Class<?> parent, Class<?> clazz) {
		return getCurrentInstance().manyToManyCascades.contains(parent.getName() + "|" + clazz.getName());
	}

	/*package*/ static final void addManyToManyCascade(Class<?> parent, Class<?> clazz) {
		getCurrentInstance().manyToManyCascades.add(parent.getName() + "|" + clazz.getName());;
	}

	/*package*/ static final void removeManyToManyCascades(Class<?> parent, Class<?> clazz) {
		getCurrentInstance().manyToManyCascades.remove(parent.getName() + "|" + clazz.getName());
	}

	/*package*/ final static Map<String, String> getAliases() {
		return getCurrentInstance().aliases;
	}

	/*package*/ final static StringBuilder getSelectBuilder() {
		return getCurrentInstance().selectBuilder;
	}

	/*package*/ final static StringBuilder getJoinBuilder() {
		return getCurrentInstance().joinBuilder;
	}

	/*package*/ final static StringBuilder getWhereBuilder() {
		return getCurrentInstance().whereBuilder;
	}

	/*package*/ final static StringBuilder getOrderBuilder() {
		return getCurrentInstance().orderBuilder;
	}

	/*package*/ final static void addBuilderValue(Object value) {
		getCurrentInstance().builderValues.add(value);
	}

	/*package*/ final static void addAllBuilderValue(Collection<Object> values) {
		getCurrentInstance().builderValues.addAll(values);
	}

	/*package*/ final static List<Object> getBuilderValues() {
		return getCurrentInstance().builderValues;
	}

	/*package*/ final static String getSQLBuilder() {
		final EntityContext context = getCurrentInstance();
		return context.selectBuilder.append(context.joinBuilder.append(context.whereBuilder.append(context.orderBuilder))).toString();
	}

	/*package*/ final static void clearSQLBuilder() {
		getCurrentInstance().clearBuilderContext();
	}

	/*package*/ final static boolean containsMaxResult() {
		EntityContext context = getCurrentInstance();
		return !context.maxResult.equals(DEFAULT) && !context.maxResult.equals(Integer.MAX_VALUE);
	}

	/*package*/ final static boolean containsFirstResult() {
		EntityContext context = getCurrentInstance();
		return !context.firstResult.equals(DEFAULT);
	}

	/*package*/ final static boolean containsOrderDir() {
		EntityContext context = getCurrentInstance();
		return context.orderDir != null;
	}

}
