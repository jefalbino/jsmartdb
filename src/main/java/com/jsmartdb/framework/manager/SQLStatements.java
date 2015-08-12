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

import java.util.HashSet;
import java.util.Set;

/*package*/ final class SQLStatements {

	/*package*/ static final Set<String> SQL_STATEMENTS = new HashSet<String>();

	static {
		SQL_STATEMENTS.add("*"); 
		SQL_STATEMENTS.add("("); 
		SQL_STATEMENTS.add(")"); 
		SQL_STATEMENTS.add("["); 
		SQL_STATEMENTS.add("]"); 
		SQL_STATEMENTS.add("?"); 
		SQL_STATEMENTS.add("%"); 
		SQL_STATEMENTS.add("="); 
		SQL_STATEMENTS.add("<"); 
		SQL_STATEMENTS.add(">"); 
		SQL_STATEMENTS.add("<>"); 
		SQL_STATEMENTS.add("!="); 
		SQL_STATEMENTS.add("<="); 
		SQL_STATEMENTS.add(">=");
		SQL_STATEMENTS.add("and"); 
		SQL_STATEMENTS.add("or"); 
		SQL_STATEMENTS.add("not");
		SQL_STATEMENTS.add("select");
		SQL_STATEMENTS.add("where");
		SQL_STATEMENTS.add("between");
		SQL_STATEMENTS.add("like");
		SQL_STATEMENTS.add("in");
		SQL_STATEMENTS.add("by");
		SQL_STATEMENTS.add("order");
		SQL_STATEMENTS.add("group");
		SQL_STATEMENTS.add("having");
		SQL_STATEMENTS.add("join");
		SQL_STATEMENTS.add("inner");
		SQL_STATEMENTS.add("left");
		SQL_STATEMENTS.add("right");
		SQL_STATEMENTS.add("outer");
		SQL_STATEMENTS.add("avg");
		SQL_STATEMENTS.add("count");
		SQL_STATEMENTS.add("max");
		SQL_STATEMENTS.add("min");
		SQL_STATEMENTS.add("sum");
		SQL_STATEMENTS.add("first");
		SQL_STATEMENTS.add("last");
		SQL_STATEMENTS.add("ucase");
		SQL_STATEMENTS.add("lcase");
		SQL_STATEMENTS.add("mid");
		SQL_STATEMENTS.add("len");
		SQL_STATEMENTS.add("round");
		SQL_STATEMENTS.add("now");
		SQL_STATEMENTS.add("format");
	}

	private SQLStatements() {
		// DO NOTHING
	}

	/*package*/ static boolean contains(String value) {
		return SQL_STATEMENTS.contains(value.toLowerCase());
	}

}
