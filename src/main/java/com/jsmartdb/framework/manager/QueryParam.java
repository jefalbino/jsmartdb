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

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class QueryParam {

	private Class<? extends Entity> entityClazz;

	private String filterName;

	private Map<String, Object> filterParams = new TreeMap<String, Object>(new QueryParamComparator());

	public QueryParam(Class<? extends Entity> entityClazz, String filterName) {
		this.entityClazz = entityClazz;
		this.filterName = filterName;
	}

	public QueryParam put(String key, Object value) {
		filterParams.put(key, value);
		return this;
	}

	/*package*/ Class<? extends Entity> getEntityClazz() {
		return entityClazz;
	}

	/*package*/ String getFilterName() {
		return filterName;
	}

	/*package*/ Object get(String key) {
		return filterParams.get(key);
	}

	/*package*/ Set<String> getFilterParamKeys() {
		return filterParams.keySet();
	}

	/*package*/ boolean isValid() {
		for (String key : getFilterParamKeys()) {
			if (key == null || key.trim().isEmpty()) {
				return false;
			}
		}
		return filterName != null && !filterName.trim().isEmpty() && entityClazz != null;
	}

	// Order parameters by its size. Bigger first
	private class QueryParamComparator implements Comparator<String> {

		@Override 
		public int compare(String string1, String string2) {
			return string1.equals(string2) ? 0 : string1.length() > string2.length() ? -1 : 1;
		}
	}

}
