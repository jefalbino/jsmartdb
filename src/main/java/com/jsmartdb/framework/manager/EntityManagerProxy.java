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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

// Proxy to manage default database connection, commit and rollback.
/*package*/ final class EntityManagerProxy implements InvocationHandler {

	private EntityManagerImpl em;

	/*package*/ EntityManagerProxy(EntityManagerImpl em) {
		this.em = em;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Object result = null;
		try {
			EntityContext.initCurrentInstance();
			result = method.invoke(em, args);

		} catch (InvocationTargetException ex) {
			EntityContext.setRollbackChanges(true);
			throw ex.getTargetException();

		} finally {
			EntityContext.closeCurrentInstance();
		}
		return result;
	}

}