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

import java.io.InputStream;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "persistence")
@XmlAccessorType(XmlAccessType.PROPERTY)
/*package*/ final class EntityPersistence {

	/*package*/ static final String[] DIALECTS = {"MySQL", "PostgreSQL"};

	/*package*/ static final String SQL_SHOW_SQL = "sql.show.sql";

	/*package*/ static final String SQL_DIALECT = "sql.dialect";

	/*package*/ static final String DATA_SOURCE_NAME = "data.source.name";

	/*package*/ static final String DRIVER_CLASS = "connection.driver.class";
	
	/*package*/ static final String CONNECTION_USERNAME = "connection.username";
	
	/*package*/ static final String CONNECTION_PASSWORD = "connection.password";
	
	/*package*/ static final String CONNECTION_URL = "connection.url";
	
	/*package*/ static final String CONNECTION_TIMEOUT = "connection.timeout";

	/*package*/ static final String CONNECTION_POOL_INITIAL_SIZE = "connection.pool.initial.size";

	/*package*/ static final String CONNECTION_POOL_MIN = "connection.pool.min.size";
	
	/*package*/ static final String CONNECTION_POOL_MAX = "connection.pool.max.size";

	/*package*/ static final String CONNECTION_MAX_STATEMENTS = "connection.max.statements";
	
	/*package*/ static final String CONNECTION_MAX_AGE = "connection.max.age";

	/*package*/ static final String BATCH_OPERATION_SIZE = "batch.operation.size";

	/*package*/ static final String TRANSACTION_ISOLATION_LEVEL = "transaction.isolation.level";

	private static Logger LOGGER = Logger.getLogger(EntityPersistence.class.getPackage().getName());

	private static EntityPersistence singleton;

	private Collection<EntityProperty> properties;

	static {
		try {
			JAXBContext context = JAXBContext.newInstance(EntityPersistence.class);
			Unmarshaller unmarshaller = context.createUnmarshaller();
			InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("jsmartdb.xml");
			singleton = (EntityPersistence) unmarshaller.unmarshal(is);
		} catch (Exception ex) {
			LOGGER.log(Level.INFO, "Failure to parse jsmartdb.xml: " + ex.getMessage());
		}
	}

	private EntityPersistence() {
		// DO NOTHING
	}

	/*package*/ static EntityPersistence getInstance() {
		return singleton;
	}

	@XmlElement(name = "property")
	@XmlElementWrapper(name = "properties")
	public Collection<EntityProperty> getProperties() {
		return properties;
	}

	public String getProperty(String name) {
		if (properties != null) {
			for (EntityProperty property : properties) {
				if (property.getName() != null && property.getName().equals(name)) {
					return property.getValue();
				}
			}
		}
		return null;
	}

	public void setProperties(Collection<EntityProperty> properties) {
		this.properties = properties;
	}

}
