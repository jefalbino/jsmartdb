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
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.List;

public abstract class Entity {

	/*package*/ String internalId;

	public Entity() {
		this.initCollections();
	}

	private void initCollections() {
		for (Field field : EntityFieldsMapper.getFields(this.getClass())) {
			if (field.getType() == List.class) {
				EntityHandler.setValue(this, field, new ArrayList<Object>());
			} else if (field.getType() == Set.class) {
				EntityHandler.setValue(this, field, new LinkedHashSet<Object>());
			}
		}
	}

    @Override
    public int hashCode() {
    	int hash = 7;
    	List<Object> ids = EntityHandler.getIdValues(this);
    	for (Object id : ids) {
			hash += 31 * hash + (id != null ? id.hashCode() : 0);
    	}
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
    	boolean equals = false;
    	if (obj != null && obj instanceof Entity) {

    		Entity that = (Entity) obj;
    		List<Object> thisIds = EntityHandler.getIdValues(this);

    		for (Object thisId : thisIds) {
    			List<Object> thatIds = EntityHandler.getIdValues(that);

    			for (Object thatId : thatIds) {
    				equals |= thisId != null && thatId != null && thatId.equals(thisId);
    			}
    		}
    	}
    	return equals;
    }

}
