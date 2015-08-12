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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;

import org.apache.commons.lang.StringUtils;

import com.jsmartdb.framework.types.JoinType;
import com.jsmartdb.framework.types.TableType;

import com.jsmartdb.framework.annotation.Column;
import com.jsmartdb.framework.annotation.Id;
import com.jsmartdb.framework.annotation.JoinId;
import com.jsmartdb.framework.annotation.ManyToMany;
import com.jsmartdb.framework.annotation.OneToMany;
import com.jsmartdb.framework.annotation.OneToOne;

/*package*/ final class EntityWhere {

	public static final String WHERE_STATEMENT = " where "; 

	public static final String AND_OPERATOR = " and ";

	public static final String OR_OPERATOR = " or ";

	/*
	 * No recursive method to get object values to place in where clause, just one join level
	 */
	@SuppressWarnings("unchecked")
	public static void getDefaultWhere(Entity entity, Field[] fields) {
		
        for (int i = 0; i < fields.length; i++) {
        	Field field = fields[i];

        	if (EntityHandler.isReserved(field) || EntityHandler.isTransient(field)) {
        		continue;
        	}

        	if (buildFieldWhere(entity, EntityAlias.getAlias(entity.getClass()), field)) {
        		continue;
        	}

        	if (!EntityContext.isBlockJoin()) {

        		JoinId joinId = EntityHandler.getJoinId(field);
        		if (joinId != null && !EntityContext.getJoinBlockClasses().contains(field.getType())) {
        			if (joinId.join().type() != JoinType.NO_JOIN) {

	            		Entity obj = (Entity) EntityHandler.getValue(entity, field);
	            		if (obj != null) {
	            			String alias = EntityAlias.getAlias(entity.getClass(), field.getType().getName(), field.getName());
	            			buildFieldsWhere(obj, alias, EntityFieldsMapper.getFields(field.getType()));
	            		}
	            	}
        			continue;
        		}

	        	OneToOne oneToOne = EntityHandler.getOneToOne(field);
	            if (oneToOne != null && !EntityContext.getJoinBlockClasses().contains(field.getType())) {
	            	if (oneToOne.join().type() != JoinType.NO_JOIN) {

	            		Entity obj = (Entity) EntityHandler.getValue(entity, field);
	            		if (obj != null) {
	            			String alias = EntityAlias.getAlias(entity.getClass(), field.getType().getName(), field.getName());
	            			buildFieldsWhere(obj, alias, EntityFieldsMapper.getFields(field.getType()));
	            		}
	            	}
	                continue;
	            }

	            OneToMany oneToMany = EntityHandler.getOneToMany(field);
                if (oneToMany != null) {
                	Class<?> oneToManyClass = EntityHandler.getGenericType(field);
                	
                	if (!EntityContext.getJoinBlockClasses().contains(oneToManyClass)) {
                		if (oneToMany.join().type() != JoinType.NO_JOIN) {

	                		if (EntityHandler.getTable(oneToManyClass).type() == TableType.DEFAULT_TABLE) {
	                    		Collection<Entity> collection = (Collection<Entity>) EntityHandler.getValue(entity, field);
	
	    	            		if (collection != null && !collection.isEmpty()) {
	    	            			EntityContext.getWhereBuilder().append("(");
	    	            			String alias = EntityAlias.getAlias(entity.getClass(), oneToManyClass.getName(), field.getName());
	
	    	            			for (Entity obj : collection) {
	    	            				EntityContext.getWhereBuilder().append("(");
	
	    	            				buildFieldsWhere(obj, alias, EntityFieldsMapper.getFields(oneToManyClass));
	
	    	            				EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - AND_OPERATOR.length(), 
	    	            						EntityContext.getWhereBuilder().length(), "").append(")" + OR_OPERATOR);
	    	            			}
	    	            			EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - OR_OPERATOR.length(), 
	    	            					EntityContext.getWhereBuilder().length(), "").append(")" + AND_OPERATOR);
	    	            		}

	                		} else {
	                		
	                			Collection<Entity> collection = (Collection<Entity>) EntityHandler.getValue(entity, field);
	
	    	            		if (collection != null && !collection.isEmpty()) {
	    	            			EntityContext.getWhereBuilder().append("(");

	    	            			String alias = EntityAlias.getAlias(entity.getClass(), oneToManyClass.getName(), field.getName());
	
	    	            			for (Entity obj : collection) {
	    	            				EntityContext.getWhereBuilder().append("(");
	
	    	            				for (Field fieldy : EntityFieldsMapper.getFields(oneToManyClass)) {
	    	            					JoinId joinIdn = EntityHandler.getJoinId(fieldy);
	    	            					
	    	            					if (buildFieldWhere(obj, alias, fieldy)) {
	    	            						continue;
	    	            					}	    		                    		
	
	    		                    		if (joinIdn != null && fieldy.getType() != entity.getClass()) {
			                    				Entity joinObj = (Entity) EntityHandler.getValue(obj, fieldy);

	    		                    			if (joinObj != null) {
	    		                    				EntityContext.getWhereBuilder().append("(");
	    		                    				String joinAlias = EntityAlias.getAlias(oneToManyClass, fieldy.getType().getName(), fieldy.getName());

	    		                    				buildFieldsWhere(joinObj, joinAlias, EntityFieldsMapper.getFields(fieldy.getType()));

	    		                    				EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - AND_OPERATOR.length(), 
	    		                    						EntityContext.getWhereBuilder().length(), "").append(")" + OR_OPERATOR);
	    		                    			}
	    		                    			continue;
	    		                    		}
	    		                    	}
	    	            				EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - AND_OPERATOR.length(), 
	    	            						EntityContext.getWhereBuilder().length(), "").append(")" + OR_OPERATOR);
	    	            			}
	    	            			EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - OR_OPERATOR.length(), 
	    	            					EntityContext.getWhereBuilder().length(), "").append(")" + AND_OPERATOR);
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
	                		Collection<Entity> collection = (Collection<Entity>) EntityHandler.getValue(entity, field);
	
		            		if (collection != null && !collection.isEmpty()) {
		            			EntityContext.getWhereBuilder().append("(");
		            			String alias = EntityAlias.getAlias(entity.getClass(), manyToManyClass.getName(), field.getName());
	
		            			for (Entity obj : collection) {
		            				EntityContext.getWhereBuilder().append("(");
		            				buildFieldsWhere(obj, alias, EntityFieldsMapper.getFields(manyToManyClass));

		            				EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - AND_OPERATOR.length(), 
		            						EntityContext.getWhereBuilder().length(), "").append(")" + OR_OPERATOR);
		            			}
		            			EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - OR_OPERATOR.length(), 
		            					EntityContext.getWhereBuilder().length(), "").append(")" + AND_OPERATOR);
		            		}
	                	}
	                	continue;
	                }
                }
        	}
        }

        if (EntityContext.getWhereBuilder().length() > WHERE_STATEMENT.length()) {
        	EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - AND_OPERATOR.length(), EntityContext.getWhereBuilder().length(), "");

        } else {
        	EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - WHERE_STATEMENT.length(), EntityContext.getWhereBuilder().length(), "");
        }
	}

	public static void getCustomWhere(Class<?> entityClazz, Field[] fields, QueryParam param) {

		String customWhere = EntityContext.getWhereBuilder().toString();
		Map<Integer, Object> paramValues = new TreeMap<Integer, Object>();

    	for (String key : param.getFilterParamKeys()) {
    		int index = 0;
    		for (int i = 0; i < StringUtils.countMatches(customWhere, key); i++) {
    			index = customWhere.indexOf(key, index);
    			paramValues.put(index, param.get(key));
    		}
    	}

    	EntityContext.addAllBuilderValue(paramValues.values());

    	for (String key : param.getFilterParamKeys()) {
    		customWhere = customWhere.replaceAll(key, " ? ");
    	}

    	String[] matches = customWhere.split(" ");
    	Map<String, String> queryMap = new HashMap<String, String>();

    	for (String match : matches) {
    		if (match.trim().isEmpty()) {
    			continue;
    		}
    		Matcher matcher = EntitySelect.SQL_FUNCTION_PATTERN.matcher(match);
			if (matcher.find()) {
				match = matcher.group();
				match = match.replace("(", "").replace(")", "");
			}

    		String value = EntityAlias.getMatchColumn(entityClazz, fields, match, EntityAlias.getAlias(entityClazz), EntityContext.getJoinBuilder());
    		if (value != null) {
    			queryMap.put(match, value);
    		}
    	}

    	for (String key : queryMap.keySet()) {
    		customWhere = customWhere.replace(key, queryMap.get(key));
    	}

    	EntityContext.getWhereBuilder().replace(0, EntityContext.getWhereBuilder().length(), customWhere);

        if (EntityContext.getWhereBuilder().length() <= WHERE_STATEMENT.length()) {
        	EntityContext.getWhereBuilder().replace(EntityContext.getWhereBuilder().length() - WHERE_STATEMENT.length(), EntityContext.getWhereBuilder().length(), "");
        }
	}

	/*package*/ static void setPreparedStatementWhereValues(PreparedStatement pstmt, boolean containsInnerSelect) throws SQLException {
		List<Object> whereBuilderValues = EntityContext.getBuilderValues();
		if (containsInnerSelect) {
			whereBuilderValues.addAll(whereBuilderValues);
		}
		for (int index = 1; index < whereBuilderValues.size() + 1; index++) {
			pstmt.setObject(index, whereBuilderValues.get(index - 1));
		}
	}

	private static void buildFieldsWhere(Entity entity, String alias, Field[] fields) {
		if (entity != null && fields != null) {
			for (Field field : fields) {
				buildFieldWhere(entity, alias, field);
	    	}
		}
	}

	private static boolean buildFieldWhere(Entity entity, String alias, Field field) {
		if (entity != null && field != null) {

			Column columnn = EntityHandler.getColumn(field);
			if (columnn != null) {
				Object obj = EntityHandler.getValue(entity, field);
				if (obj != null) {
					EntityContext.addBuilderValue(obj);
					EntityContext.getWhereBuilder().append(alias + "." + columnn.name() + "= ?" + AND_OPERATOR);
					return true;
				}
			}

			Id id = EntityHandler.getId(field);
			if (id != null) {
				Object obj = EntityHandler.getValue(entity, field);
				if (obj != null) {
					EntityContext.addBuilderValue(obj);
					EntityContext.getWhereBuilder().append(alias + "." + id.name() + "= ?" + AND_OPERATOR);
					return true;
				}
			}

			JoinId joinId = EntityHandler.getJoinId(field);
			if (joinId != null) {
				Entity obj = (Entity) EntityHandler.getValue(entity, field);
				if (obj != null) {
					try {
						Field refererField = EntityFieldsMapper.getField(obj.getClass(), joinId.referer());
						EntityContext.addBuilderValue(EntityHandler.getValue(obj, refererField));
						EntityContext.getWhereBuilder().append(alias + "." + joinId.column() + "= ?" + AND_OPERATOR);
						return true;
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			}
		}
		return false;
	}

}
