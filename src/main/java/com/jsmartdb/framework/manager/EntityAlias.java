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
import java.util.ArrayList;
import java.util.List;

import com.jsmartdb.framework.types.JoinType;
import com.jsmartdb.framework.types.TableType;

import com.jsmartdb.framework.annotation.Column;
import com.jsmartdb.framework.annotation.Id;
import com.jsmartdb.framework.annotation.JoinId;
import com.jsmartdb.framework.annotation.ManyToMany;
import com.jsmartdb.framework.annotation.OneToMany;
import com.jsmartdb.framework.annotation.OneToOne;

/*package*/ final class EntityAlias {

	private static final String PARENT_SEPARATOR = "|";

	private static final String FIELD_SEPARATOR = "#";

	public static String getAlias(Class<?> clazz) {
		return EntityContext.getAliases().get(PARENT_SEPARATOR + clazz.getName() + FIELD_SEPARATOR);
	}

	public static void setAlias(Class<?> clazz, String value) {
		EntityContext.getAliases().put(PARENT_SEPARATOR + clazz.getName() + FIELD_SEPARATOR, value); 
	}

	public static String getAlias(Class<?> clazz, String targetClass, String fieldName) {
		return EntityContext.getAliases().get(clazz.getName() + PARENT_SEPARATOR + targetClass + FIELD_SEPARATOR + fieldName);
	}

	public static void setAlias(Class<?> clazz, String targetClass, String fieldName, String value) {
		EntityContext.getAliases().put(clazz.getName() + PARENT_SEPARATOR + targetClass + FIELD_SEPARATOR + fieldName, value);
	}

	public static Class<?> getTargetClass(String aliasKey) {
		try {
			return Class.forName(aliasKey.substring(aliasKey.indexOf(PARENT_SEPARATOR) + 1, aliasKey.indexOf(FIELD_SEPARATOR)));
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}

	public static String getMatchColumn(Class<?> entityClazz, Field[] fields, String match, String alias, StringBuilder joinBuilder) {
		if (match != null && !match.trim().isEmpty() && !SQLStatements.contains(match)) {
			for (Field field : fields) {

				if (match.startsWith(field.getName())) {

					if (match.contains(".")) {
						if (!EntityContext.isBlockJoin()) {

							try {
		    					Class<?> clazz = entityClazz;
		    					String[] fieldMatches = match.split("\\.");
		    					StringBuilder matchBuilder = new StringBuilder();
	
		    					for (int i = 0; i < fieldMatches.length; i++) {
		    						
		    						Field matchField = EntityFieldsMapper.getField(clazz, fieldMatches[i]);
	
		    						Id id = EntityHandler.getId(matchField);
			    					if (id != null) {
			    						matchBuilder.append((entityClazz != clazz ? "" : alias) + "." + id.name());
			    						continue;
			    					}
	
		    						JoinId joinId = EntityHandler.getJoinId(matchField);
		    						if (joinId != null) {
		    							matchBuilder.append(getAlias(clazz, matchField.getType().getName(), fieldMatches[i]));
		    							clazz = matchField.getType();
		    							continue;
		    						}
	
		    						Column column = EntityHandler.getColumn(matchField);
		    						if (column != null) {
		    							matchBuilder.append((entityClazz != clazz ? "" : alias) + "." + column.name());
		    							continue;
		    						}
	
		    						OneToOne oneToOne = EntityHandler.getOneToOne(matchField);
		    						if (oneToOne != null && !EntityContext.getJoinBlockClasses().contains(clazz)) {
		    							if (oneToOne.join().type() != JoinType.NO_JOIN) {

			    							String targetMatchAlias = getAlias(clazz, matchField.getType().getName(), matchField.getName());
	
			    							if (targetMatchAlias == null) {
			    								String targetTable = EntityHandler.getTable(field.getType()).name();
			    								targetMatchAlias = targetTable + EntityContext.getAliasCounter();
	
			    						    	String targetField = EntityHandler.getId(EntityFieldsMapper.getField(field.getType(), oneToOne.joinColumn().referer())).name();
	
			    						    	setAlias(clazz, matchField.getType().getName(), field.getName(), targetMatchAlias);
	
			    						    	joinBuilder.append(EntityJoin.buildOneToOneSQL(oneToOne.join().type(), targetTable, targetMatchAlias, targetField, alias, oneToOne.joinColumn().column()));
			    							}
	
			    							matchBuilder.replace(0, matchBuilder.length(), targetMatchAlias);
			    							clazz = matchField.getType();
		    							}
		    							continue;
		    						}
	
		    						OneToMany oneToMany = EntityHandler.getOneToMany(matchField);
		    						if (oneToMany != null && !EntityContext.getJoinBlockClasses().contains(clazz)) {
		    							if (oneToMany.join().type() != JoinType.NO_JOIN) {

			    							Class<?> oneToManyClass = EntityHandler.getGenericType(matchField);
			    							String targetMatchAlias = getAlias(clazz, oneToManyClass.getName(), matchField.getName());

			    							if (targetMatchAlias == null) {

			    								String targetTable = EntityHandler.getTable(oneToManyClass).name();
				    							targetMatchAlias = targetTable + EntityContext.getAliasCounter();
	
						                    	setAlias(clazz, oneToManyClass.getName(), field.getName(), targetMatchAlias);

				    							if (EntityHandler.getTable(oneToManyClass).type() == TableType.DEFAULT_TABLE) {

				    								Id referenceId = EntityHandler.getId(EntityFieldsMapper.getField(oneToManyClass, oneToMany.joinColumn().referer()));
						                        	String targetField;
						                        	if (referenceId != null) {
						                        		targetField = referenceId.name();
						                        	} else {
						                        		targetField = EntityHandler.getColumn(EntityFieldsMapper.getField(oneToManyClass, oneToMany.joinColumn().referer())).name();
						                        	}

						                        	Field fieldId = EntityFieldsMapper.getField(clazz, oneToMany.joinColumn().column());

							                    	joinBuilder.append(EntityJoin.buildOneToManySQL(oneToMany.join().type(), targetTable, targetMatchAlias, targetField, alias, EntityHandler.getId(fieldId).name()));

				    							} else {
				    								
				    								String sourceTable = EntityHandler.getTable(entityClazz).name();
				    	                			List<EntityJoinTable> joinTables = new ArrayList<EntityJoinTable>();

				    	                			Field targetField = EntityJoin.buildJoinIdTables(EntityFieldsMapper.getFields(oneToManyClass), sourceTable, targetMatchAlias, joinTables, false);

				    	                			JoinId targetJoinId = EntityHandler.getJoinId(targetField);
			    	                    			Field fieldId = EntityFieldsMapper.getField(targetField.getType(), targetJoinId.referer());

				    		                    	for (EntityJoinTable entityJoinTable : joinTables) {

				    	                    			String joinTableAlias = entityJoinTable.getTable() + EntityContext.getAliasCounter();

				    	                    			EntityAlias.setAlias(oneToManyClass, entityJoinTable.getField().getType().getName(), entityJoinTable.getField().getName(), joinTableAlias);
				    	                    			
				    	                    			Field refererField = EntityFieldsMapper.getField(entityJoinTable.getField().getType(), entityJoinTable.getJoinId().referer());

				    	                    			joinBuilder.append(EntityJoin.buildJoinIdSQL(oneToMany.join().type(), targetTable, targetMatchAlias, targetJoinId.column(), 
				    	                    					entityJoinTable.getJoinId().column(), alias, EntityHandler.getId(fieldId).name(), entityJoinTable.getTable(), joinTableAlias, 
				    	                    					EntityHandler.getId(refererField).name()));
				    		                    	}
				    							}
			    							}

			    							if (EntityHandler.getTable(oneToManyClass).type() == TableType.DEFAULT_TABLE || i == fieldMatches.length -2) {
			    								matchBuilder.replace(0, matchBuilder.length(), targetMatchAlias);
			    							}
			    							clazz = oneToManyClass;
		    							}
		    							continue;
		    						}
	
		    						ManyToMany manyToMany = EntityHandler.getManyToMany(matchField);
		    						if (manyToMany != null && !EntityContext.getJoinBlockClasses().contains(clazz)) {
		    							if (manyToMany.join().type() != JoinType.NO_JOIN) {

			    							Class<?> manyToManyClass = EntityHandler.getGenericType(matchField);
			    							String targetMatchAlias = getAlias(clazz, manyToManyClass.getName(), matchField.getName());
	
			    							if (targetMatchAlias == null) {
			    								
			    								String targetTableOne = manyToMany.joinTable();
			    		                    	String targetTableTwo = EntityHandler.getTable(manyToManyClass).name();
	
			    		                    	String aliasCounter = EntityContext.getAliasCounter();
	
			    		                    	String targetAliasOne = targetTableOne + aliasCounter;
			    		                    	targetMatchAlias = targetTableTwo + aliasCounter;
	
			    		                    	setAlias(clazz, manyToManyClass.getName(), field.getName(), targetMatchAlias);
	
			    		                    	Field refererField =  EntityFieldsMapper.getField(clazz, manyToMany.joinColumn().referer());
			    		                    	Field inverserRefererField = EntityFieldsMapper.getField(manyToManyClass, manyToMany.inverseJoinColumn().referer());
	
				    	                    	joinBuilder.append(EntityJoin.buildManyToManySQL(manyToMany.join().type(), targetTableOne, targetAliasOne, manyToMany.joinColumn().column(),
				    	                    			targetTableTwo, targetMatchAlias, EntityHandler.getId(inverserRefererField).name(), manyToMany.inverseJoinColumn().column(), alias, EntityHandler.getId(refererField).name()));
			    							}
	
			    							matchBuilder.replace(0, matchBuilder.length(), targetMatchAlias);
			    							clazz = manyToManyClass;
		    							}
		    							continue;
		    						}
		    					}

		    					return matchBuilder.toString();

							} catch (Exception ex) {
								throw new RuntimeException(ex.getMessage());
							}

						} else {
							throw new RuntimeException("Invalid QueryFilter! While block join is activated the query cannot contains operator . (point)!");
						}

					} else {
						try {
							Field matchField = EntityFieldsMapper.getField(entityClazz, match);
	
							Id id = EntityHandler.getId(matchField);
							if (id != null) {
								return EntityAlias.getAlias(entityClazz) + "." + id.name();
							}
	
							Column column = EntityHandler.getColumn(matchField);
							if (column != null) {
								return EntityAlias.getAlias(entityClazz) + "." + column.name();
							}

						} catch (Exception ex) {
							throw new RuntimeException(ex.getMessage());
						}
					}
				}
			}
		}
		return null;
	}

}
