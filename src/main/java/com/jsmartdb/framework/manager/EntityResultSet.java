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
import java.sql.ResultSet;
import java.sql.SQLException;

import com.jsmartdb.framework.types.JoinType;
import com.jsmartdb.framework.types.TableType;

import com.jsmartdb.framework.annotation.Column;
import com.jsmartdb.framework.annotation.Id;
import com.jsmartdb.framework.annotation.JoinId;
import com.jsmartdb.framework.annotation.ManyToMany;
import com.jsmartdb.framework.annotation.OneToMany;
import com.jsmartdb.framework.annotation.OneToOne;

/*package*/ final class EntityResultSet {

	/*package*/ static boolean reuseEntity(final Entity entity, final ResultSet rs, final String alias) throws Exception {
		boolean entityMatch = false;
		if (entity != null) {
			for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {

				if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {
					String columnName = null;
	
					Id id = EntityHandler.getId(field);
					if (id != null) {
						columnName = alias + id.name();
					}
	
					Column column = EntityHandler.getColumn(field);
					if (column != null) {
						columnName = alias + column.name();
					}
	
					if (columnName != null) {
						entityMatch = true;
	
						Object entityValue = EntityHandler.getValue(entity, field);
						Object resultSetValue = getResultSetValue(rs, columnName, field.getClass());
	
						if ((entityValue == null && resultSetValue != null) || (entityValue != null && resultSetValue == null)) {
							return false;
						}
	
						if (!entityValue.equals(resultSetValue)) {
							return false;
						}
	
					} else {
						JoinId joinId = EntityHandler.getJoinId(field);
						if (joinId != null) {
							Entity joinEntity = (Entity) EntityHandler.getValue(entity, field);
	
							if (joinEntity != null) {
								String joinAlias = EntityAlias.getAlias(entity.getClass(), joinEntity.getClass().getName(), field.getName());

								if (!reuseEntity(joinEntity, rs, joinAlias)) {
									return false;
								}
							}
				        }
					}
				}
			}
		}
		return entityMatch;
	}

	/*package*/ static Entity createEntity(final Class<? extends Entity> clazz, final ResultSet rs) {
    	Entity entity = null;
    	try {
    		entity = clazz.newInstance();
    		EntityHandler.setInternalId(entity);
        	setResultSet(entity, rs, EntityAlias.getAlias(clazz));

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
    	return entity;
    }

	/*package*/ static void setResultSet(final Entity entity, final ResultSet rs, final String alias) {
		try {
			if (EntityHandler.isJoinTable(entity.getClass())) {
				EntityContext.addBuildBlockedClass(entity.getClass());
			}

			for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {

				if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {
					String columnName = null;

					Id id = EntityHandler.getId(field);
					if (id != null) {
						columnName = alias + id.name();
					}

					Column column = EntityHandler.getColumn(field);
					if (column != null) {
						columnName = alias + column.name();
					}

					if (columnName != null) {
						EntityHandler.setValue(entity, field, getResultSetValue(rs, columnName, field.getClass()));

					} else {
						setJoinResultSet(entity, field, rs);
					}
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static void setJoinResultSet(final Entity entity, final ResultSet rs) {
		if (!EntityContext.isBlockJoin()) {
			for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {

				if (!EntityHandler.isReserved(field) && !EntityHandler.isId(field) && !EntityHandler.isColumn(field) && !EntityHandler.isTransient(field)) {
					setJoinResultSet(entity, field, rs);
				}
			}
		}
	}

	private static boolean setJoinResultSet(final Entity entity, final Field field, final ResultSet rs) {
		if (!EntityContext.isBlockJoin()) {

		    OneToOne oneToOne = EntityHandler.getOneToOne(field);
		    if (oneToOne != null && !EntityContext.getJoinBlockClasses().contains(field.getType())) {
		    	if (oneToOne.join().type() != JoinType.NO_JOIN) {
		    		setJoinResultSet(entity, field, oneToOne, rs);
		    	}
		        return true;
		    }

		    OneToMany oneToMany = EntityHandler.getOneToMany(field);
	        if (oneToMany != null) {
	        	Class<?> oneToManyClass = EntityHandler.getGenericType(field);

	        	if (!EntityContext.getJoinBlockClasses().contains(oneToManyClass) && !EntityContext.containsBuildBlockedClass(oneToManyClass)) {
		        	if (oneToMany.join().type() != JoinType.NO_JOIN) {
		        		setJoinResultSet(entity, field, oneToMany, oneToManyClass, rs);
		        	}
		            return true;
	        	}
	        }

	        ManyToMany manyToMany = EntityHandler.getManyToMany(field);
	        if (manyToMany != null) {
	        	Class<?> manyToManyClass = EntityHandler.getGenericType(field);

	        	if (!EntityContext.getJoinBlockClasses().contains(manyToManyClass)) {
		        	if (manyToMany.join().type() != JoinType.NO_JOIN) {
		        		setJoinResultSet(entity, field, manyToMany, manyToManyClass, rs);
		        	}
		            return true;
	        	}
	        }

	        JoinId joinId = EntityHandler.getJoinId(field);
	        if (joinId != null) {
	        	if (!EntityContext.containsBuildBlockedClass(field.getType())) {
	        		setJoinResultSet(entity, field, joinId, rs);
	        	} else {
	        		EntityContext.setBlockedField(field);
	        	}
		        return true;
	        }
		}
        return false;
	}

	private static void setJoinResultSet(final Entity entity, final Field field, final JoinId joinId, final ResultSet rs) {
		try {
	        String alias = EntityAlias.getAlias(entity.getClass(), field.getType().getName(), field.getName());
	        if (alias != null) {
		        Field refererField = EntityFieldsMapper.getField(field.getType(), joinId.referer());
	
		        if (rs.getObject(alias + EntityHandler.getRefererName(refererField), SQLTypes.getSQLTypes()) != null) {
	
	            	if (EntityHandler.getValue(entity, field) == null) {
	            		Entity obj = (Entity) field.getType().newInstance();
	            		EntityHandler.setInternalId(obj);
	
	            		setResultSet(obj, rs, alias);
	                    EntityHandler.setValue(entity, field, obj);
	
	                    EntityContext.setBuildJoinedField(true);
	            	}
	            }
	        }
	    } catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (SQLException ex) {
			// DO NOTHING
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private static void setJoinResultSet(final Entity entity, final Field field, final OneToOne oneToOne, final ResultSet rs) {
	    try {
	        String alias = EntityAlias.getAlias(entity.getClass(), field.getType().getName(), field.getName());
	        if (alias != null) {
		        Field refererField = EntityFieldsMapper.getField(field.getType(), oneToOne.joinColumn().referer());
	
	            if (rs.getObject(alias + EntityHandler.getRefererName(refererField), SQLTypes.getSQLTypes()) != null) {
	
	            	Entity obj = (Entity) EntityHandler.getValue(entity, field);
	
	            	if (obj == null) {
	            		obj = (Entity) field.getType().newInstance();
	            		EntityHandler.setInternalId(obj);
	
	            		setResultSet(obj, rs, alias);
	                    EntityHandler.setValue(entity, field, obj);
	
	            	} else {
	            		setJoinResultSet(obj, rs);
	            	}
	            }
	        }
	    } catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (SQLException ex) {
			// DO NOTHING
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private static void setJoinResultSet(final Entity entity, final Field field, final OneToMany oneToMany, final Class<?> oneToManyClass, final ResultSet rs) {
		try {
			if (EntityHandler.getTable(oneToManyClass).type() == TableType.DEFAULT_TABLE) {

				String alias = EntityAlias.getAlias(entity.getClass(), oneToManyClass.getName(), field.getName());
				if (alias != null) {
					Field nameField = EntityFieldsMapper.getField(oneToManyClass, EntityHandler.getFirstIdName(oneToManyClass));
	
				    Object id = rs.getObject(alias + EntityHandler.getFirstIdName(oneToManyClass), SQLTypes.getSQLTypes());
	
				    if (id != null) {
						Entity obj = EntityHandler.getCollectionValue(entity, field, id, nameField);
	
						if (obj == null) {
							obj = (Entity) oneToManyClass.newInstance();
							EntityHandler.setInternalId(obj);
	
							setResultSet(obj, rs, alias);
							EntityHandler.addValue(entity, field, obj);
	
						} else {
							setJoinResultSet(obj, rs);
						}
					}
				}

			} else { // Case JOIN_TABLE for ManyToMany relationship
				Entity obj = (Entity) oneToManyClass.newInstance();
				EntityHandler.setInternalId(obj);

				String alias = EntityAlias.getAlias(entity.getClass(), oneToManyClass.getName(), field.getName());
				if (alias != null) {
					EntityContext.addBuildBlockedClass(entity.getClass());
	
					setResultSet(obj, rs, alias);
	
					if (EntityContext.isBuildJoinedField()) {
						EntityHandler.setValue(obj, EntityContext.getBuildBlockedField(), entity);
	
						if (!EntityHandler.containsObject(entity, field, obj)) {
							EntityHandler.addValue(entity, field, obj);
						}
					}
				}
			}
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (SQLException ex) {
			// DO NOTHING
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private static void setJoinResultSet(final Entity entity, final Field field, final ManyToMany manyToMany, final Class<?> manyToManyClass, final ResultSet rs) {
		try {
			String alias = EntityAlias.getAlias(entity.getClass(), manyToManyClass.getName(), field.getName());
			if (alias != null) {
				Field refererField = EntityFieldsMapper.getField(manyToManyClass, manyToMany.inverseJoinColumn().referer());
	
				Object id = rs.getObject(alias + EntityHandler.getRefererName(refererField), SQLTypes.getSQLTypes());
	
				if (id != null) {
					Entity obj = EntityHandler.getCollectionValue(entity, field, id, refererField);
	
					if (obj == null) {
						obj = (Entity) manyToManyClass.newInstance();
						EntityHandler.setInternalId(obj);
	
						setResultSet(obj, rs, alias);
						EntityHandler.addValue(entity, field, obj);
	
					} else {
						setJoinResultSet(obj, rs);
					}
				}
			}
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		} catch (InstantiationException ex) {
			throw new RuntimeException(ex);
		} catch (SQLException ex) {
			// DO NOTHING
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static Object getResultSetValue(final ResultSet rs, final int columnIndex, final Class<?> fieldClass) throws SQLException {
		if (fieldClass == Byte.class) {
        	return new Byte(rs.getByte(columnIndex));

        } else if (fieldClass == Short.class) {
        	return new Short(rs.getShort(columnIndex));

        } else if (fieldClass == Integer.class) {
        	return new Integer(rs.getInt(columnIndex));

        } else if (fieldClass == Long.class) {
        	return new Long(rs.getLong(columnIndex));

        } else if (fieldClass == Float.class) {
        	return new Float(rs.getFloat(columnIndex));

        } else if (fieldClass == Double.class) {
        	return new Double(rs.getDouble(columnIndex));

        } else {
        	return rs.getObject(columnIndex, SQLTypes.getSQLTypes());
        }
	}

	/*package*/ static Object getResultSetValue(final ResultSet rs, final String columnName, final Class<?> fieldClass) throws SQLException {
		if (fieldClass == Byte.class) {
        	return new Byte(rs.getByte(columnName));

        } else if (fieldClass == Short.class) {
        	return new Short(rs.getShort(columnName));

        } else if (fieldClass == Integer.class) {
        	return new Integer(rs.getInt(columnName));

        } else if (fieldClass == Long.class) {
        	return new Long(rs.getLong(columnName));

        } else if (fieldClass == Float.class) {
        	return new Float(rs.getFloat(columnName));

        } else if (fieldClass == Double.class) {
        	return new Double(rs.getDouble(columnName));

        } else {
        	return rs.getObject(columnName, SQLTypes.getSQLTypes());
        }
	}

}
