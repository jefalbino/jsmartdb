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

import java.util.Collection;

import com.jsmartdb.framework.annotation.ManyToMany;

/*package*/ class EntityManyToMany {

	private ManyToMany manyToMany;

	private Collection<Entity> collection;

	public ManyToMany getManyToMany() {
		return manyToMany;
	}

	public void setManyToMany(ManyToMany manyToMany) {
		this.manyToMany = manyToMany;
	}

	public Collection<Entity> getCollection() {
		return collection;
	}

	public void setCollection(Collection<Entity> collection) {
		this.collection = collection;
	}

}
