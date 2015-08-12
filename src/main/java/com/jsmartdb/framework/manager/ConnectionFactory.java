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

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/*package*/ final class ConnectionFactory {

	/*package*/ static Integer BATCH_SIZE = 1000;

	private static Integer TRANSACTION_ISOLATION_LEVEL = null;

	private static String DATA_SOURCE_NAME;

    private static Context INIT_CONTEXT;

    private static Context ENV_CONTEXT;

    private static DataSource dataSource;

    private static ComboPooledDataSource connectionPool;

    static {
    	EntityPersistence persistence = EntityPersistence.getInstance();
    	
    	DATA_SOURCE_NAME = persistence.getProperty(EntityPersistence.DATA_SOURCE_NAME);

	   	String batchSize = persistence.getProperty(EntityPersistence.BATCH_OPERATION_SIZE);
		if (batchSize != null) {
			try {
				BATCH_SIZE = Integer.parseInt(batchSize);
			} catch (NumberFormatException ex) {
				ex.printStackTrace();
			}
		}

		String transactionIsolation = persistence.getProperty(EntityPersistence.TRANSACTION_ISOLATION_LEVEL);
		if (transactionIsolation != null) {
			try {
				TRANSACTION_ISOLATION_LEVEL = Integer.parseInt(transactionIsolation);
			} catch (NumberFormatException ex) {
				ex.printStackTrace();
			}
		}

    	try {
    		INIT_CONTEXT = new InitialContext();
    		ENV_CONTEXT = (Context) INIT_CONTEXT.lookup("java:/comp/env");
    	} catch (NamingException ex) {
    		// ex.printStackTrace();
    	}
    }

    /*package*/ static final Connection getConnection() {
        try {
        	Connection connection = null;

        	if (DATA_SOURCE_NAME != null) {
        		try {
	    			if (dataSource == null) {
	    				if (ENV_CONTEXT != null) {
	    					dataSource = (DataSource) ENV_CONTEXT.lookup(DATA_SOURCE_NAME);
	        			} else {
	        				dataSource = (DataSource) INIT_CONTEXT.lookup(DATA_SOURCE_NAME);
	        			}
	    			}
	
	                connection = dataSource.getConnection();

	    		} catch (NamingException ex) {
	    			throw new RuntimeException(ex.getMessage());
	    		}
        	}

        	if (connection == null) {

        		if (connectionPool == null) {
        			EntityPersistence persistence = EntityPersistence.getInstance();

        	    	connectionPool = new ComboPooledDataSource();
        	    	connectionPool.setJdbcUrl(persistence.getProperty(EntityPersistence.CONNECTION_URL));
        	    	connectionPool.setUser(persistence.getProperty(EntityPersistence.CONNECTION_USERNAME));
        	    	connectionPool.setPassword(persistence.getProperty(EntityPersistence.CONNECTION_PASSWORD));
        	    	connectionPool.setDriverClass(persistence.getProperty(EntityPersistence.DRIVER_CLASS));

        	    	String timeOut = persistence.getProperty(EntityPersistence.CONNECTION_TIMEOUT);
        	    	if (timeOut != null) {
        	    		connectionPool.setCheckoutTimeout(Integer.parseInt(timeOut));
        	    	}

        	    	String poolInitialSize = persistence.getProperty(EntityPersistence.CONNECTION_POOL_INITIAL_SIZE);
        	    	if (poolInitialSize != null) {
        	    		connectionPool.setInitialPoolSize(Integer.parseInt(poolInitialSize));
        	    	}

        	    	String poolMinimumSize = persistence.getProperty(EntityPersistence.CONNECTION_POOL_MIN);
        	    	if (poolMinimumSize != null) {
        	    		connectionPool.setMinPoolSize(Integer.parseInt(poolMinimumSize));
        	    	}

        	    	String poolMaximumSize = persistence.getProperty(EntityPersistence.CONNECTION_POOL_MAX);
        	    	if (poolMaximumSize != null) {
        	    		connectionPool.setMaxPoolSize(Integer.parseInt(poolMaximumSize));
        	    	}

        	    	String poolMaximumStatements = persistence.getProperty(EntityPersistence.CONNECTION_MAX_STATEMENTS);
        	    	if (poolMaximumStatements != null) {
        	    		connectionPool.setMaxStatements(Integer.parseInt(poolMaximumStatements));
        	    	}

        	    	String poolMaximumAge = persistence.getProperty(EntityPersistence.CONNECTION_MAX_AGE);
        	    	if (poolMaximumAge != null) {
        	    		connectionPool.setMaxConnectionAge(Integer.parseInt(poolMaximumAge));
        	    	}
        		}

        		connection = connectionPool.getConnection();
        	}

			if (connection != null) {
				connection.setAutoCommit(false);
				if (TRANSACTION_ISOLATION_LEVEL != null) {
					connection.setTransactionIsolation(TRANSACTION_ISOLATION_LEVEL);
				}
            } else {
            	throw new RuntimeException("Connection closed!");
            }

			return connection;

        } catch (Exception ex) {
        	throw new RuntimeException(ex.getMessage());
        }
    }

    /*package*/ static final void putConnection(Connection connection, boolean rollbackChanges) {
        try {
            if (connection != null) {
            	if (rollbackChanges) {
            		connection.rollback();
            	} else {
            		connection.commit();
            	}
            }
        } catch (SQLException ex) {
        	throw new RuntimeException(ex.getMessage());

        } finally {
        	try {
	        	if (connection != null) {
	        		connection.close();
	        	}
        	} catch (SQLException ex) {
        		throw new RuntimeException(ex.getMessage());
        	}
        }
    }

}
