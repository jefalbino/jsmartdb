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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jsmartdb.framework.annotation.Column;
import com.jsmartdb.framework.annotation.Id;
import com.jsmartdb.framework.annotation.JoinId;
import com.jsmartdb.framework.annotation.ManyToMany;
import com.jsmartdb.framework.annotation.OneToOne;

/*package*/ final class EntityUpdate {

	private static final Map<String, String> INSERT_PSTMT_CACHE = new HashMap<String, String>();

	private static final Map<String, String> UPDATE_PSTMT_CACHE = new HashMap<String, String>();

	private static final String AND_OPERATOR = " and ";

	/*package*/ static PreparedStatement getInsert(Class<?> clazz) {
		PreparedStatement pstmt = null;
		try {
			if (EntityFieldsMapper.getFields(clazz).length > 0) {

				String sql = INSERT_PSTMT_CACHE.get(clazz.getName());

				if (sql == null) {
					StringBuilder builder1 = new StringBuilder("insert into " + EntityHandler.getTable(clazz).name() + " (");
					StringBuilder builder2 = new StringBuilder(" values (");
	
					for (Field field : EntityFieldsMapper.getFields(clazz)) {
						if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field) && !EntityHandler.isOneToMany(field) && !EntityHandler.isManyToMany(field)) {
	
							Column column = EntityHandler.getColumn(field);
							if (column != null) {
								builder1.append(column.name() + ",");
								builder2.append("?,");
								continue;
							}
	
							OneToOne oneToOne = EntityHandler.getOneToOne(field);
							if (oneToOne != null) {
								builder1.append(oneToOne.joinColumn().column() + ",");
								builder2.append("?,");
								continue;
							}
	
							Id id = EntityHandler.getId(field);
							if (id != null) {
								builder1.append(id.name() + ",");
								builder2.append("?,");
								continue;
							}
						}
					}
	
					sql = builder1.substring(0, builder1.length() -1) + ")" + builder2.substring(0, builder2.length() -1) + ")";
	
					INSERT_PSTMT_CACHE.put(clazz.getName(), sql);
				}

				pstmt = EntityContext.getConnection().prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}

	/*package*/ static void putInsertValues(PreparedStatement pstmt, Entity entity) {
		try {
			if (pstmt != null) {
				int paramIndex = 1;
	
				for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
					if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field) && !EntityHandler.isOneToMany(field) && !EntityHandler.isManyToMany(field)) {
	
						Column column = EntityHandler.getColumn(field);
						if (column != null) {
							pstmt.setObject(paramIndex++, EntityHandler.getValue(entity, field));
							continue;
						}
	
						OneToOne oneToOne = EntityHandler.getOneToOne(field);
						if (oneToOne != null) {
							Entity ent = (Entity) EntityHandler.getValue(entity, field);
							pstmt.setObject(paramIndex++, ent != null ? EntityHandler.getRefererValue(ent, EntityFieldsMapper.getField(ent.getClass(), oneToOne.joinColumn().referer())) : null);
							continue;
						}

						Id id = EntityHandler.getId(field);
						if (id != null) {
							if (id.sequence().isEmpty()) {
								pstmt.setObject(paramIndex++, EntityHandler.getValue(entity, field));
							} else {
								pstmt.setObject(paramIndex++, id.sequence() + ".nextval");
							}
							continue;
						}

					}
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static PreparedStatement getInsertJoin(Class<?> clazz) {
		PreparedStatement pstmt = null;
		try {
			if (EntityFieldsMapper.getFields(clazz).length > 0) {

				String sql = INSERT_PSTMT_CACHE.get(clazz.getName());

				if (sql == null) {
					StringBuilder builder1 = new StringBuilder("insert into " + EntityHandler.getTable(clazz).name() + " (");
					StringBuilder builder2 = new StringBuilder(" values (");
	
					for (Field field : EntityFieldsMapper.getFields(clazz)) {
						if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {
	
							JoinId joinId = EntityHandler.getJoinId(field);
							if (joinId != null) {
								builder1.append(joinId.column() + ",");
								builder2.append("?,");
								continue;
							}
	
							Column column = EntityHandler.getColumn(field);
							if (column != null) {
								builder1.append(column.name() + ",");
								builder2.append("?,");
								continue;
							}
						}
					}
	
					sql = builder1.substring(0, builder1.length() -1) + ")" + builder2.substring(0, builder2.length() -1) + ")";
	
					INSERT_PSTMT_CACHE.put(clazz.getName(), sql);
				}

				pstmt = EntityContext.getConnection().prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS);
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}
	
	/*package*/ static void putInsertJoinValues(PreparedStatement pstmt, Entity entity) {
		try {
			if (pstmt != null) {
				int paramIndex = 1;
	
				for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
					if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {
	
						JoinId joinId = EntityHandler.getJoinId(field);
						if (joinId != null) {
							Entity ent = (Entity) EntityHandler.getValue(entity, field);
							pstmt.setObject(paramIndex++, EntityHandler.getRefererValue(ent, EntityFieldsMapper.getField(ent.getClass(), joinId.referer())));
							continue;
						}
	
						Column column = EntityHandler.getColumn(field);
						if (column != null) {
							pstmt.setObject(paramIndex++, EntityHandler.getValue(entity, field));
							continue;
						}
					}
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static PreparedStatement getInsertJoin(Class<?> classOne, Class<?> classTwo) {
		PreparedStatement pstmt = null;
		try {
			String sqlKey = classOne.getName() + classTwo.getName();
			String sql = INSERT_PSTMT_CACHE.get(sqlKey);

			if (sql == null) {
				ManyToMany manyToMany = EntityHandler.getManyToMany(EntityFieldsMapper.getFields(classOne), classTwo);
				if (manyToMany != null) {
					sql = "insert into " + manyToMany.joinTable() + " (" + manyToMany.joinColumn().column() + "," + manyToMany.inverseJoinColumn().column() + ") values (?,?)";
					INSERT_PSTMT_CACHE.put(sqlKey, sql);
				}
			}

			pstmt = EntityContext.getConnection().prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS);

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}

	/*package*/ static void putInsertJoinValues(PreparedStatement pstmt, ManyToMany manyToMany, Entity entityOne, Entity entityTwo) {
		try {
			if (pstmt != null) {
				pstmt.setObject(1, EntityHandler.getRefererValue(entityOne, 
						EntityFieldsMapper.getField(entityOne.getClass(), manyToMany.joinColumn().referer())));
				pstmt.setObject(2, EntityHandler.getRefererValue(entityTwo, 
						EntityFieldsMapper.getField(entityTwo.getClass(), manyToMany.inverseJoinColumn().referer())));
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static PreparedStatement getUpdate(Class<?> clazz) {
		PreparedStatement pstmt = null;
		try {
			if (EntityFieldsMapper.getFields(clazz).length > 0) {

	            String sql = UPDATE_PSTMT_CACHE.get(clazz.getName());

	            if (sql == null) {
					StringBuilder builder1 = new StringBuilder("update " + EntityHandler.getTable(clazz).name() + " set ");
					StringBuilder builder2 = new StringBuilder(EntityWhere.WHERE_STATEMENT);

					for (Field field : EntityFieldsMapper.getFields(clazz)) {
						if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field) && !EntityHandler.isOneToMany(field) && !EntityHandler.isManyToMany(field)) {

							Column column = EntityHandler.getColumn(field);
							if (column != null) {
								builder1.append(column.name() + "= ?,");
								continue;
							}

							OneToOne oneToOne = EntityHandler.getOneToOne(field);
							if (oneToOne != null) {
								builder1.append(oneToOne.joinColumn().column() + "= ?,");
								continue;
							}
	
							Id id = EntityHandler.getId(field);
							if (id != null) {
								builder1.append(id.name() + "= ?,");
								builder2.append(id.name() + "= ?" + AND_OPERATOR);
								continue;
							}
						}
					}
	
					sql = builder1.substring(0, builder1.length() -1) + builder2.substring(0, builder2.length() - AND_OPERATOR.length());
	
					UPDATE_PSTMT_CACHE.put(clazz.getName(), sql);
	            }

				pstmt = EntityContext.getConnection().prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS);
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}

	/*package*/ static void putUpdateValues(PreparedStatement pstmt, Entity entity) {
		try {
			if (pstmt != null) {
				int paramIndex = 1;
				List<Object> updateValues = new ArrayList<Object>();
				
				for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
					if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field) && !EntityHandler.isOneToMany(field) && !EntityHandler.isManyToMany(field)) {

						Column column = EntityHandler.getColumn(field);
						if (column != null) {
							pstmt.setObject(paramIndex++, EntityHandler.getValue(entity, field));
							continue;
						}

						OneToOne oneToOne = EntityHandler.getOneToOne(field);
						if (oneToOne != null) {
							Entity ent = (Entity) EntityHandler.getValue(entity, field);
							pstmt.setObject(paramIndex++, ent != null ? EntityHandler.getRefererValue(ent, EntityFieldsMapper.getField(ent.getClass(), oneToOne.joinColumn().referer())) : null);
							continue;
						}

						Id id = EntityHandler.getId(field);
						if (id != null) {
							Object value = EntityHandler.getValue(entity, field);
							pstmt.setObject(paramIndex++, value);
							updateValues.add(value);
							continue;
						}
					}
				}

				// Set primary key value in where clause
				for (Object value : updateValues) {
					pstmt.setObject(paramIndex++, value);
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static PreparedStatement getUpdateJoin(Class<?> clazz) {
		PreparedStatement pstmt = null;
		try {
			if (EntityFieldsMapper.getFields(clazz).length > 0) {

				String sql = UPDATE_PSTMT_CACHE.get(clazz.getName());

				if (sql == null) {
					StringBuilder builder1 = new StringBuilder("update " + EntityHandler.getTable(clazz).name() + " set ");
					StringBuilder builder2 = new StringBuilder(EntityWhere.WHERE_STATEMENT);

					for (Field field : EntityFieldsMapper.getFields(clazz)) {
						if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {

							Column column = EntityHandler.getColumn(field);
							if (column != null) {
								builder1.append(column.name() + "= ?,");
								continue;
							}

							JoinId joinId = EntityHandler.getJoinId(field);
							if (joinId != null) {
								builder1.append(joinId.column() + "= ?,");
								builder2.append(joinId.column() + "= ?" + AND_OPERATOR);
								continue;
							}
						}
					}

					sql = builder1.substring(0, builder1.length() -1) + builder2.substring(0, builder2.length() - AND_OPERATOR.length());

					UPDATE_PSTMT_CACHE.put(clazz.getName(), sql);
				}

				pstmt = EntityContext.getConnection().prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS);
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}

	/*package*/ static void putUpdateJoinValues(PreparedStatement pstmt, Entity entity) {
		try {
			if (pstmt != null) {
				int paramIndex = 1;
				List<Object> updateValues = new ArrayList<Object>();

				for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
					if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {

						Column column = EntityHandler.getColumn(field);
						if (column != null) {
							pstmt.setObject(paramIndex++, EntityHandler.getValue(entity, field));
							continue;
						}

						JoinId joinId = EntityHandler.getJoinId(field);
						if (joinId != null) {
							Entity ent = (Entity) EntityHandler.getValue(entity, field);
							Object value = EntityHandler.getRefererValue(ent, EntityFieldsMapper.getField(ent.getClass(), joinId.referer()));
							pstmt.setObject(paramIndex++, value);
							updateValues.add(value);
							continue;
						}
					}
				}

				// Set primary key value in where clause
				for (Object value : updateValues) {
					pstmt.setObject(paramIndex++, value);
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static PreparedStatement getDelete(Entity entity) {
		PreparedStatement pstmt = null;
		try {
			if (EntityFieldsMapper.getFields(entity.getClass()).length > 0) {

				StringBuilder builder1 = new StringBuilder("delete from " + EntityHandler.getTable(entity.getClass()).name());
				StringBuilder builder2 = new StringBuilder();

				for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
					if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {

						Id id = EntityHandler.getId(field);
						if (id != null && EntityHandler.getValue(entity, field) != null) {
							builder2.append(id.name() + "= ?" + AND_OPERATOR);
							continue;
						}
		
						Column column = EntityHandler.getColumn(field);
						if (column != null && EntityHandler.getValue(entity, field) != null) {
							builder2.append(column.name() + "= ?" + AND_OPERATOR);
							continue;
						}
					}
				}

				if (builder2.length() == 0) {
					throw new RuntimeException("Object to be deleted must have at least one attribute non transient filled!");
				}

				String sql = builder1.append(EntityWhere.WHERE_STATEMENT) + builder2.substring(0, builder2.length() - AND_OPERATOR.length());
				pstmt = EntityContext.getConnection().prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS);
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}

	/*package*/ static void putDeleteValues(PreparedStatement pstmt, Entity entity) {
		try {
			if (pstmt != null) {
				int paramIndex = 1;
				
				for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
					if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {

						Id id = EntityHandler.getId(field);
						if (id != null) {
							Object value = EntityHandler.getValue(entity, field);
							if (value != null) {
								pstmt.setObject(paramIndex++, value);
							}
							continue;
						}

						Column column = EntityHandler.getColumn(field);
						if (column != null) {
							Object value = EntityHandler.getValue(entity, field);
							if (value != null) {
								pstmt.setObject(paramIndex++, value);
							}
							continue;
						}
					}
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static PreparedStatement getDeleteJoin(Entity entity) {
		PreparedStatement pstmt = null;
		try {
			if (EntityFieldsMapper.getFields(entity.getClass()).length > 0) {

				StringBuilder builder1 = new StringBuilder("delete from " + EntityHandler.getTable(entity.getClass()).name());
				StringBuilder builder2 = new StringBuilder();

				for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
					if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {

						JoinId joinId = EntityHandler.getJoinId(field);
						if (joinId != null && EntityHandler.getValue(entity, field) != null) {
							builder2.append(joinId.column() + "= ?" + AND_OPERATOR);
							continue;
						}

						Column column = EntityHandler.getColumn(field);
						if (column != null && EntityHandler.getValue(entity, field) != null) {
							builder2.append(column.name() + "= ?" + AND_OPERATOR);
							continue;
						}
					}
				}

				if (builder2.length() == 0) {
					throw new RuntimeException("Object to be deleted must have at least one attribute non transient filled!");
				}

				String sql = builder1.append(EntityWhere.WHERE_STATEMENT) + builder2.substring(0, builder2.length() - AND_OPERATOR.length());
				pstmt = EntityContext.getConnection().prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS);
			}

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}

	/*package*/ static void putDeleteJoinValues(PreparedStatement pstmt, Entity entity) {
		try {
			if (pstmt != null) {
				int paramIndex = 1;

				for (Field field : EntityFieldsMapper.getFields(entity.getClass())) {
					if (!EntityHandler.isReserved(field) && !EntityHandler.isTransient(field)) {

						JoinId joinId = EntityHandler.getJoinId(field);
						if (joinId != null) {
							Entity ent = (Entity) EntityHandler.getValue(entity, field);
							if (ent != null) {
								pstmt.setObject(paramIndex++, EntityHandler.getRefererValue(ent, EntityFieldsMapper.getField(ent.getClass(), joinId.referer())));
							}
							continue;
						}

						Column column = EntityHandler.getColumn(field);
						if (column != null) {
							Object value = EntityHandler.getValue(entity, field);
							if (value != null) {
								pstmt.setObject(paramIndex++, value);
							}
							continue;
						}
					}
				}
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static PreparedStatement getDeleteJoin(Class<?> classOne, Class<?> classTwo) {
		PreparedStatement pstmt = null;
		try {
            String sql = null;

			ManyToMany manyToMany = EntityHandler.getManyToMany(EntityFieldsMapper.getFields(classOne), classTwo);
			if (manyToMany != null) {
				sql = "delete from " + manyToMany.joinTable() + EntityWhere.WHERE_STATEMENT + manyToMany.joinColumn().column() + "= ?" + AND_OPERATOR + manyToMany.inverseJoinColumn().column() + "= ?";
			}

			pstmt = EntityContext.getConnection().prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS);

		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}

	/*package*/ static void putDeleteJoinValues(PreparedStatement pstmt, ManyToMany manyToMany, Entity entityOne, Entity entityTwo) {
		try {
			if (pstmt != null) {
				pstmt.setObject(1, EntityHandler.getRefererValue(entityOne, 
						EntityFieldsMapper.getField(entityOne.getClass(), manyToMany.joinColumn().referer())));
				pstmt.setObject(2, EntityHandler.getRefererValue(entityTwo, 
						EntityFieldsMapper.getField(entityTwo.getClass(), manyToMany.inverseJoinColumn().referer())));
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/*package*/ static PreparedStatement getNative(String query) {
		PreparedStatement pstmt = null;
		try {
			if (query != null) {
				pstmt = EntityContext.getConnection().prepareStatement(query, 
						query.toLowerCase().startsWith("insert") ? PreparedStatement.RETURN_GENERATED_KEYS : PreparedStatement.NO_GENERATED_KEYS);
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
		return pstmt;
	}

	/*package*/ static void putNativeValues(PreparedStatement pstmt, Object[] values) {
		try {
			if (pstmt != null && values != null) {
				int paramIndex = 1;
				for (Object value : values) {
					pstmt.setObject(paramIndex++, value);
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}

}
