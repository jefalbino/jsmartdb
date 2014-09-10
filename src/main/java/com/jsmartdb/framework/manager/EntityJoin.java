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
import java.util.List;

import com.jsmartdb.framework.types.JoinType;

import com.jsmartdb.framework.annotation.Column;
import com.jsmartdb.framework.annotation.Id;
import com.jsmartdb.framework.annotation.JoinId;

/*package*/ final class EntityJoin {

	public static String buildOneToOneSQL(JoinType type, String target, String targetAlias, String targetField, String sourceAlias, String sourceField) {
		String join = " left outer join ";
		if (type == JoinType.INNER_JOIN) {
			join = " inner join ";
		}
		return join + target + " as " + targetAlias + " on " + sourceAlias + "." + sourceField + "=" + targetAlias + "." + targetField;
	}

	public static String buildOneToManySQL(JoinType type, String target, String targetAlias, String targetField, String sourceAlias, String sourceField) {
		String join = " left outer join ";
		if (type == JoinType.INNER_JOIN) {
			join = " inner join ";
		}
		return join + target + " as " + targetAlias + " on " + sourceAlias + "." + sourceField + "=" + targetAlias + "." + targetField;
	}

	public static String buildJoinIdSQL(JoinType type, String target, String targetAlias, String targetField, String targetInverseField, 
										 	String sourceAlias, String sourceField, String inverseTarget, String inverseTargetAlias, String inverseTargetField) {
		String join = " left outer join ";
		if (type == JoinType.INNER_JOIN) {
			join = " inner join ";
		}
		return join + target + " as " + targetAlias + " on " + sourceAlias + "." + sourceField + "=" + targetAlias + "." + targetField 
				+ join + inverseTarget + " as " + inverseTargetAlias + " on " + targetAlias + "." + targetInverseField + "=" + inverseTargetAlias + "." + inverseTargetField;
	}

	public static String buildManyToManySQL(JoinType type, String targetOne, String targetOneAlias, String targetOneField, 
												String targetTwo, String targetTwoAlias, String targetTwoField, String targetTwoJoinField, String sourceAlias, String sourceField) {
		String join = " left outer join ";
		if (type == JoinType.INNER_JOIN) {
			join = " inner join ";
		}
		return join + targetOne + " as " + targetOneAlias + " on " + sourceAlias + "." + sourceField + "=" + targetOneAlias + "." + targetOneField
				+ join + targetTwo + " as " + targetTwoAlias + " on " + targetOneAlias + "." + targetTwoJoinField + "=" + targetTwoAlias + "." + targetTwoField;
	}

	public static Field buildJoinIdTables(Field[] fields, String sourceTable, String targetAlias, List<EntityJoinTable> joinTables, boolean isDefaultQuery) {
		Field targetField = null;

		for (Field field : fields) {
			JoinId joinId = EntityHandler.getJoinId(field);

			if (joinId != null) {
				String targetTable = EntityHandler.getTable(field.getType()).name();

				if (targetTable.equals(sourceTable)) {
					targetField = field;
				} else {
					EntityJoinTable joinTable = new EntityJoinTable();
					joinTable.setTable(targetTable);
					joinTable.setField(field);
					joinTable.setJoinId(joinId);
					joinTables.add(joinTable);
				}
				continue;
			}

			if (isDefaultQuery) {
				Column columnn = EntityHandler.getColumn(field);
	    		if (columnn != null) {
	    			EntityContext.getSelectBuilder().append(targetAlias + "." + columnn.name() + " as " + targetAlias + columnn.name() + ",");
	    			continue;
	    		}
			}
		}

		return targetField;
	}

	public static void buildJoinOneLevelSQL(Field[] fields, String targetAlias) {
    	for (Field field : fields) {

    		Column columnn = EntityHandler.getColumn(field);
    		if (columnn != null) {
    			EntityContext.getSelectBuilder().append(targetAlias + "." + columnn.name() + " as " + targetAlias + columnn.name() + ",");
    			continue;
    		}

    		Id idn = EntityHandler.getId(field);
			if (idn != null) {
				EntityContext.getSelectBuilder().append(targetAlias + "." + idn.name() + " as " + targetAlias + idn.name() + ",");
    			continue;
    		}
    	}
	}

	public static String getSelectInnerJoinSQL(String joinAsName, StringBuilder joinBuilder) {
		int index = joinBuilder.indexOf("inner join " + joinAsName);
		if (index < 0) {
			return null;
		}

		String joinChunk = joinBuilder.substring(index);
		index = joinChunk.indexOf("left outer join ", joinChunk.indexOf("on"));
		if (index < 0) {
			index = joinChunk.indexOf("inner join ", joinChunk.indexOf("on"));
		}

		if (index > 0) {
			return joinChunk.substring(0, index);
		}
		return joinChunk;
	}

}
