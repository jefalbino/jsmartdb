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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/*package*/ final class SQLTypes {

	private static final Map<String, Class<?>> MYSQL_TYPES = new HashMap<String, Class<?>>();

	private static String DIALECT = EntityPersistence.getInstance().getProperty(EntityPersistence.SQL_DIALECT);

	static { // All MySQL data type can be found here: http://dev.mysql.com/doc/refman/5.0/en/data-type-overview.html
		MYSQL_TYPES.put("BIT", Boolean.class);
		MYSQL_TYPES.put("BOOL", Byte.class);
		MYSQL_TYPES.put("BOOLEAN", Byte.class);
		MYSQL_TYPES.put("TINYINT", Short.class);
		MYSQL_TYPES.put("SMALLINT", Short.class);
		MYSQL_TYPES.put("INT", Integer.class);        
		MYSQL_TYPES.put("MEDIUMINT", Integer.class);
		MYSQL_TYPES.put("INTEGER", Integer.class);
        MYSQL_TYPES.put("LONG", Long.class);
        MYSQL_TYPES.put("BIGINT", Long.class);
        MYSQL_TYPES.put("REAL", Float.class);
        MYSQL_TYPES.put("FLOAT", Double.class);
        MYSQL_TYPES.put("DOUBLE", Double.class);
        MYSQL_TYPES.put("DOUBLE PRECISION", Double.class);
        MYSQL_TYPES.put("DEC", BigDecimal.class);
        MYSQL_TYPES.put("DECIMAL", BigDecimal.class);
        MYSQL_TYPES.put("NUMERIC", BigDecimal.class);
        MYSQL_TYPES.put("DATE", Date.class);
        MYSQL_TYPES.put("DATETIME", Date.class);
        MYSQL_TYPES.put("TIME", Time.class);
        MYSQL_TYPES.put("TIMESTAMP", Timestamp.class);
        MYSQL_TYPES.put("CHAR", String.class);
        MYSQL_TYPES.put("VARCHAR", String.class);
        MYSQL_TYPES.put("LONGVARCHAR", String.class);
        MYSQL_TYPES.put("TEXT", String.class);
    }

	private SQLTypes() {
		// DO NOTHING
	}

	/*package*/ static Map<String, Class<?>> getSQLTypes() {
		if (DIALECT.equals(EntityPersistence.DIALECTS[0])) {
			return MYSQL_TYPES;
		}
		return null;
	}

}
