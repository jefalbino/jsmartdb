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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.jsmartdb.framework.types.JoinLevel;
import com.jsmartdb.framework.types.JoinType;
import com.jsmartdb.framework.types.OrderType;
import com.jsmartdb.framework.types.TableType;

import com.jsmartdb.framework.annotation.Column;
import com.jsmartdb.framework.annotation.Id;
import com.jsmartdb.framework.annotation.JoinId;
import com.jsmartdb.framework.annotation.ManyToMany;
import com.jsmartdb.framework.annotation.OneToMany;
import com.jsmartdb.framework.annotation.OneToOne;
import com.jsmartdb.framework.annotation.QueryFilter;
import com.jsmartdb.framework.annotation.Table;

/*package*/ final class EntitySelect {

	public static final String SELECT_STATEMENT = "select";

	public static final String DISTINCT_STATEMENT = " distinct ";

	public static final String SELECT_DISTINCT_STATEMENT = SELECT_STATEMENT + DISTINCT_STATEMENT;

	public static final String FROM_STATEMENT = " from ";

	public static final String IN_STATEMENT = " in ";

	public static final String LIMIT_STATEMENT = " limit ";

	public static final String OFFSET_STATEMENT = " offset ";

	public static final String ORDER_BY_STATEMENT = " order by ";

	public static final String SEPARATOR = ", ";

	public static final Pattern SQL_FUNCTION_PATTERN = Pattern.compile("\\(.*\\)");

	/*package*/ static PreparedStatement getSelectQuery(Entity entity) {
		try {
		    String targetTable = EntityHandler.getTable(entity.getClass()).name();
	
		    String targetAlias = targetTable + EntityContext.getAliasCounter();
	
		    EntityAlias.setAlias(entity.getClass(), targetAlias);
	
		    EntityContext.getJoinBuilder().append(targetTable + " as " + targetAlias);
	
		    getDefaultSelect(entity.getClass(), entity.getClass(), EntityFieldsMapper.getFields(entity.getClass()), null, targetAlias);
	
		    EntityWhere.getDefaultWhere(entity, EntityFieldsMapper.getFields(entity.getClass()));
	
		    if (EntityContext.containsMaxResult()) {
	
		    	if (EntityContext.getWhereBuilder().length() > EntityWhere.WHERE_STATEMENT.length()) {
		    		EntityContext.getWhereBuilder().append(EntityWhere.AND_OPERATOR);
		    	} else {
		    		EntityContext.getWhereBuilder().append(EntityWhere.WHERE_STATEMENT);
		    	}
	
		    	EntityContext.getWhereBuilder().append(getInnerSelect(entity.getClass()));
		    }
	
		    getOrderBy(entity.getClass(), EntityContext.getJoinBuilder(), EntityContext.getOrderBuilder());
	
			return getPreparedStatement();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private static PreparedStatement getPreparedStatement() {
		PreparedStatement pstmt = null;
		try {
			pstmt = EntityContext.getConnection().prepareStatement(EntityContext.getSQLBuilder());
			EntityWhere.setPreparedStatementWhereValues(pstmt, EntityContext.containsMaxResult());

		} catch (SQLException ex) {
			throw new RuntimeException(ex);

		} finally {
			EntityContext.clearSQLBuilder();
		}
		return pstmt;
	}

	private static void getDefaultSelect(Class<?> entityClazz, Class<?> fieldClazz, Field[] fields, String matchField, String alias) throws Exception {
		for (int i = 0; i < fields.length; i++) {
        	Field field = fields[i];

        	// For custom select queries
        	if (matchField != null) {
        		if (!matchField.startsWith(field.getName())) {
        			continue;
        		}
        		matchField = matchField.contains(".") ? matchField.substring(matchField.indexOf(".")) : matchField;
        	}

        	if (EntityHandler.isReserved(field) || EntityHandler.isTransient(field)) {
        		continue;
        	}

        	Column column = EntityHandler.getColumn(field);
            if (column != null) {
            	EntityContext.getSelectBuilder().append(alias + "." + column.name() + (matchField == null ? " as " + alias + column.name() + "," : ","));
            	continue;
            }

        	Id id = EntityHandler.getId(field);
            if (id != null) {
            	EntityContext.getSelectBuilder().append(alias + "." + id.name() + (matchField == null ? " as " + alias + id.name() + "," : ","));
            	continue;
            }

            JoinId joinId = EntityHandler.getJoinId(field);
            if (joinId != null) {
            	EntityContext.getSelectBuilder().append(alias + "." + joinId.column() + (matchField == null ? " as " + alias + joinId.column() + "," : ","));
            }

        	if (!EntityContext.isBlockJoin()) {

        		if (joinId != null && !EntityContext.getJoinBlockClasses().contains(field.getType())) {
        			
        			if (joinId.join().type() != JoinType.NO_JOIN) {

        				Field[] joinIdFields = EntityFieldsMapper.getFields(field.getType());

                		String targetTable = EntityHandler.getTable(field.getType()).name();
                    	String targetAlias = targetTable + EntityContext.getAliasCounter();

                    	String targetField = EntityHandler.getId(EntityFieldsMapper.getField(field.getType(), joinId.referer())).name();

                    	EntityAlias.setAlias(fieldClazz, field.getType().getName(), field.getName(), targetAlias);

                    	EntityContext.getJoinBuilder().append(EntityJoin.buildOneToOneSQL(joinId.join().type(), targetTable, targetAlias, targetField, alias, joinId.column()));

                    	if (joinId.join().level() == JoinLevel.JOIN_ALL_LEVELS) {
                        	getDefaultSelect(entityClazz, field.getType(), joinIdFields, matchField, targetAlias);
                    	} else {
                    		EntityJoin.buildJoinOneLevelSQL(joinIdFields, targetAlias);
                        }
        			}
        			continue;
        		}
        		
                OneToOne oneToOne = EntityHandler.getOneToOne(field);
                if (oneToOne != null && !EntityContext.getJoinBlockClasses().contains(field.getType())) {

                	if (oneToOne.join().type() != JoinType.NO_JOIN) {

                		Field[] oneToOneFields = EntityFieldsMapper.getFields(field.getType());

                		String targetTable = EntityHandler.getTable(field.getType()).name();
                    	String targetAlias = targetTable + EntityContext.getAliasCounter();

                    	String targetField = EntityHandler.getId(EntityFieldsMapper.getField(field.getType(), oneToOne.joinColumn().referer())).name();

                    	EntityAlias.setAlias(fieldClazz, field.getType().getName(), field.getName(), targetAlias);

                    	EntityContext.getJoinBuilder().append(EntityJoin.buildOneToOneSQL(oneToOne.join().type(), targetTable, targetAlias, targetField, alias, oneToOne.joinColumn().column()));

                    	if (oneToOne.join().level() == JoinLevel.JOIN_ALL_LEVELS) {
                        	getDefaultSelect(entityClazz, field.getType(), oneToOneFields, matchField, targetAlias);
                    	} else {
                    		EntityJoin.buildJoinOneLevelSQL(oneToOneFields, targetAlias);
                        }
                	}
                    continue;
                }

                OneToMany oneToMany = EntityHandler.getOneToMany(field);
                if (oneToMany != null) {
                	Class<?> oneToManyClass = EntityHandler.getGenericType(field);

                	if (!EntityContext.getJoinBlockClasses().contains(oneToManyClass)) {

                		if (oneToMany.join().type() != JoinType.NO_JOIN) {

                			String targetTable = EntityHandler.getTable(oneToManyClass).name();
                			String targetAlias = targetTable + EntityContext.getAliasCounter();

                			EntityAlias.setAlias(fieldClazz, oneToManyClass.getName(), field.getName(), targetAlias);

                			if (EntityHandler.getTable(oneToManyClass).type() == TableType.DEFAULT_TABLE) {

	                			try {
		                        	Id refererId = EntityHandler.getId(EntityFieldsMapper.getField(oneToManyClass, oneToMany.joinColumn().referer()));
		                        	String targetField;
		                        	if (refererId != null) {
		                        		targetField = refererId.name();
		                        	} else {
		                        		targetField = EntityHandler.getColumn(EntityFieldsMapper.getField(oneToManyClass, oneToMany.joinColumn().referer())).name();
		                        	}

		                        	Field fieldId = EntityFieldsMapper.getField(fieldClazz, oneToMany.joinColumn().column());

		                        	EntityContext.getJoinBuilder().append(EntityJoin.buildOneToManySQL(oneToMany.join().type(), targetTable, targetAlias, 
		                        			targetField, alias, EntityHandler.getId(fieldId).name()));

			                        Field[] oneToManyFields = EntityFieldsMapper.getFields(oneToManyClass);

			                        if (oneToMany.join().level() == JoinLevel.JOIN_ALL_LEVELS) {
			                        	getDefaultSelect(entityClazz, oneToManyClass, oneToManyFields, matchField, targetAlias);
			                        } else {
			                        	EntityJoin.buildJoinOneLevelSQL(oneToManyFields, targetAlias);
			                        }

	                			} catch (Exception ex) {
	                				throw new RuntimeException(ex);
	                        	}

                			// JOIN TABLE
	                		} else {

	                			String sourceTable = EntityHandler.getTable(entityClazz).name();
	                			List<EntityJoinTable> joinTables = new ArrayList<EntityJoinTable>();

	                			Field targetField = EntityJoin.buildJoinIdTables(EntityFieldsMapper.getFields(oneToManyClass), sourceTable, targetAlias, joinTables, true);
	                			
	                			JoinId targetJoinId = EntityHandler.getJoinId(targetField);
	                			Field fieldId = EntityFieldsMapper.getField(targetField.getType(), targetJoinId.referer());
	                			
		                    	for (EntityJoinTable entityJoinTable : joinTables) {

	                    			String joinTableAlias = entityJoinTable.getTable() + EntityContext.getAliasCounter();

	                    			EntityAlias.setAlias(oneToManyClass, entityJoinTable.getField().getType().getName(), entityJoinTable.getField().getName(), joinTableAlias);

	                    			Field[] oneToManyFields = EntityFieldsMapper.getFields(entityJoinTable.getField().getType());
	                    			Field refererFieldId = EntityFieldsMapper.getField(entityJoinTable.getField().getType(), entityJoinTable.getJoinId().referer());

	                    			EntityContext.getJoinBuilder().append(EntityJoin.buildJoinIdSQL(oneToMany.join().type(), targetTable, targetAlias, targetJoinId.column(), 
	                    					entityJoinTable.getJoinId().column(), alias, EntityHandler.getId(fieldId).name(), entityJoinTable.getTable(), joinTableAlias, 
	                    					EntityHandler.getId(refererFieldId).name()));
	
	                    			if (oneToMany.join().level() == JoinLevel.JOIN_ALL_LEVELS) {
	    	                        	getDefaultSelect(entityClazz, entityJoinTable.getField().getType(), oneToManyFields, matchField, joinTableAlias);
	                    			} else {
	                    				EntityJoin.buildJoinOneLevelSQL(oneToManyFields, joinTableAlias);
	                    			}
		                    	}
	                		}
                		}
	                    continue;
	                }
                }

                ManyToMany manyToMany = EntityHandler.getManyToMany(field);
                if (manyToMany != null) {
                	Class<?> manyToManyClass = EntityHandler.getGenericType(field);

                	if (!EntityContext.getJoinBlockClasses().contains(manyToManyClass)) {

	                	if (manyToMany.join().type() != JoinType.NO_JOIN) {
	
	                    	String targetTableOne = manyToMany.joinTable();
	                    	String targetTableTwo = EntityHandler.getTable(manyToManyClass).name();

	                    	String aliasCounter = EntityContext.getAliasCounter();

	                    	String targetAliasOne = targetTableOne + aliasCounter;
	                    	String targetAliasTwo = targetTableTwo + aliasCounter;

	                    	EntityAlias.setAlias(fieldClazz, manyToManyClass.getName(), field.getName(), targetAliasTwo);
	
	                    	Field refererIdField = EntityFieldsMapper.getField(manyToManyClass, manyToMany.joinColumn().referer());
	                    	Field inverseRefererField = EntityFieldsMapper.getField(manyToManyClass, manyToMany.inverseJoinColumn().referer());
	           
	                    	EntityContext.getJoinBuilder().append(EntityJoin.buildManyToManySQL(manyToMany.join().type(), targetTableOne, targetAliasOne, manyToMany.joinColumn().column(),
	                    			targetTableTwo, targetAliasTwo, EntityHandler.getId(inverseRefererField).name(), manyToMany.inverseJoinColumn().column(), alias, EntityHandler.getId(refererIdField).name()));
	
	                    	if (manyToMany.join().level() == JoinLevel.JOIN_ALL_LEVELS) {
	                    		getDefaultSelect(entityClazz, manyToManyClass, EntityFieldsMapper.getFields(manyToManyClass), matchField, targetAliasTwo);
	                    	} else {
	                    		EntityJoin.buildJoinOneLevelSQL(EntityFieldsMapper.getFields(manyToManyClass), targetAliasTwo);
	                    	}
	                	}
	                    continue;
	                }
                }
        	}
        }

		// Remove last ,
	    if (fieldClazz == entityClazz) {
	    	EntityContext.getSelectBuilder().deleteCharAt(EntityContext.getSelectBuilder().length() - 1);
	    }
	}

	private static StringBuilder getInnerSelect(Class<?> entityClazz) {
		Field[] fields = EntityFieldsMapper.getFields(entityClazz);

		String selectId = EntityHandler.isJoinTable(entityClazz) ? EntityHandler.getFirstJoinIdName(entityClazz) : EntityHandler.getFirstIdName(entityClazz);

		StringBuilder innerSelectBuilder = new StringBuilder(EntityAlias.getAlias(entityClazz) + "." + selectId + IN_STATEMENT + "(" + SELECT_DISTINCT_STATEMENT + selectId + FROM_STATEMENT + "(");

		StringBuilder innerJoinBuilder = new StringBuilder(EntityHandler.getTable(entityClazz).name() + " as " + EntityAlias.getAlias(entityClazz) + SEPARATOR);

		for (Entry<String, String> alias : EntityContext.getAliases().entrySet()) {
			if (EntityContext.getWhereBuilder().indexOf(alias.getValue()) >= 0 && innerJoinBuilder.indexOf(alias.getValue()) < 0) {

				String joinTableName = EntityHandler.getTable(EntityAlias.getTargetClass(alias.getKey())).name() + " as " + alias.getValue();
				String joinChunk = EntityJoin.getSelectInnerJoinSQL(joinTableName, EntityContext.getJoinBuilder());

				if (joinChunk != null && !joinChunk.isEmpty()) {
					innerJoinBuilder.replace(innerJoinBuilder.length() - SEPARATOR.length(), innerJoinBuilder.length(), "").append(" " + joinChunk + SEPARATOR);
				} else {
					innerJoinBuilder.append(joinTableName + SEPARATOR);
				}
			}
		}

		if (innerJoinBuilder.length() > SEPARATOR.length()) {
			innerJoinBuilder.replace(innerJoinBuilder.length() - SEPARATOR.length(), innerJoinBuilder.length(), "");
		}

		String orderByField = EntityAlias.getMatchColumn(entityClazz, fields, EntityContext.getOrderBy(), EntityAlias.getAlias(entityClazz), innerJoinBuilder);

		innerSelectBuilder.append(SELECT_DISTINCT_STATEMENT + EntityAlias.getAlias(entityClazz) + "." + selectId + (orderByField == null ? "" : "," + orderByField) + FROM_STATEMENT + innerJoinBuilder);

		StringBuilder innerWhereBuilder = null;

		if (EntityContext.getWhereBuilder().length() > EntityWhere.WHERE_STATEMENT.length()) {
			innerWhereBuilder = new StringBuilder(EntityContext.getWhereBuilder().substring(0, EntityContext.getWhereBuilder().length() - EntityWhere.AND_OPERATOR.length()));
		} else {
			innerWhereBuilder = new StringBuilder(EntityContext.getWhereBuilder().substring(0, EntityContext.getWhereBuilder().length() - EntityWhere.WHERE_STATEMENT.length()));
		}

		StringBuilder innderOrderByBuilder = new StringBuilder(ORDER_BY_STATEMENT);

		getOrderBy(entityClazz, innerJoinBuilder, innderOrderByBuilder);

		innerSelectBuilder.append(innerWhereBuilder.toString() + innderOrderByBuilder);

		if (EntityContext.containsMaxResult()) {
			innerSelectBuilder.append(LIMIT_STATEMENT + EntityContext.getMaxResult());
		}

		if (EntityContext.containsFirstResult()) {
			innerSelectBuilder.append(OFFSET_STATEMENT + EntityContext.getFirstResult());
		}
		
		return innerSelectBuilder.append(") as " + EntityHandler.getTable(entityClazz).name() + "_temp_limit )");
	}

	/*package*/ static PreparedStatement getCustomSelectQuery(Class<?> entityClazz, QueryFilter queryFilter, QueryParam param) {
		try {
			String targetTable = EntityHandler.getTable(entityClazz).name();
	
		    String targetAlias = targetTable + EntityContext.getAliasCounter();  
	
		    EntityAlias.setAlias(entityClazz, targetAlias);
	
		    EntityContext.getJoinBuilder().append(targetTable + " as " + targetAlias);
	
	        if (!queryFilter.select().trim().isEmpty()) {
	        	getCustomSelect(entityClazz, entityClazz, EntityFieldsMapper.getFields(entityClazz), queryFilter.select(), targetAlias);
	        } else {
	        	getDefaultSelect(entityClazz, entityClazz, EntityFieldsMapper.getFields(entityClazz), null, targetAlias);
	        }
	
	        EntityContext.getWhereBuilder().append(queryFilter.where());
	
	        EntityWhere.getCustomWhere(entityClazz, EntityFieldsMapper.getFields(entityClazz), param);
	
	        if (EntityContext.containsMaxResult()) {
	
		    	if (EntityContext.getWhereBuilder().length() > EntityWhere.WHERE_STATEMENT.length()) {
		    		EntityContext.getWhereBuilder().append(EntityWhere.AND_OPERATOR);
		    	} else {
		    		EntityContext.getWhereBuilder().append(EntityWhere.WHERE_STATEMENT);
		    	}
	
		    	EntityContext.getWhereBuilder().append(getInnerSelect(entityClazz));
		    }
	
		    getOrderBy(entityClazz, EntityContext.getJoinBuilder(), EntityContext.getOrderBuilder());
	
			return getPreparedStatement();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private static void getCustomSelect(Class<?> entityClazz, Class<?> clazz, Field[] fields, String select, String alias) {
		try {
			String[] selects = select.split(",");
	
			for (int i = 0; i < selects.length; i++) {
				Matcher matcher = SQL_FUNCTION_PATTERN.matcher(selects[i]);
	
				String match = null;
				if (matcher.find()) {
					match = matcher.group();
				}
	
				if (match != null) {
					EntityContext.getSelectBuilder().append(selects[i].replace(match, "("));
				}
	
				getDefaultSelect(entityClazz, entityClazz, EntityFieldsMapper.getFields(entityClazz), 
						(match != null ? match.replace("(", "").replace(")", "") : selects[i]), alias);
	
				if (match != null) {
					EntityContext.getSelectBuilder().append(")");
				}
	
				EntityContext.getSelectBuilder().append(i < selects.length -1 ? "," : "");
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private static void getOrderBy(Class<?> entityClazz, StringBuilder joinBuilder, StringBuilder orderByBuilder) {		
		Table targetTable = EntityHandler.getTable(entityClazz);

		String orderBy = EntityAlias.getMatchColumn(entityClazz, EntityFieldsMapper.getFields(entityClazz), EntityContext.getOrderBy(), EntityAlias.getAlias(entityClazz), joinBuilder);

		if (orderBy != null && !orderBy.trim().isEmpty()) {
			orderByBuilder.append(orderBy + " ");
		} else if (!targetTable.orderBy().trim().isEmpty()) {
			orderByBuilder.append(EntityAlias.getAlias(entityClazz) + "." + targetTable.orderBy() + " ");
		} else {
			orderByBuilder.replace(0, orderByBuilder.length(), "");
		}

		if (orderByBuilder.length() != 0) {
			if (EntityContext.containsOrderDir()) {
				orderByBuilder.append((EntityContext.getOrderDir() == OrderType.ASCENDING ? "asc" : "desc") + " ");
			} else {
				orderByBuilder.append((targetTable.orderDir() == OrderType.ASCENDING ? "asc" : "desc") + " ");
			}
		}
	}

}