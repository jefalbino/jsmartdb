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
import java.util.HashMap;
import java.util.Map;

/*package*/ final class EntityFieldsMapper {

	private static Map<Class<?>, Field[]> mappedFields = new HashMap<Class<?>, Field[]>();

	private EntityFieldsMapper() {
		// DO NOTHING
	}

	/*package*/ static Field[] getFields(final Class<?> clazz) {
		if (!mappedFields.containsKey(clazz)) {
			mappedFields.put(clazz, clazz.getDeclaredFields());
		}
		return mappedFields.get(clazz);
	}

	/*package*/ static Field getField(final Class<?> clazz, final String name) throws Exception {
		String internedName = name.intern();
		for (Field field : getFields(clazz)) {
			if (field.getName() == internedName) {
				return field;
			}
		}
		return null;
	}

}
