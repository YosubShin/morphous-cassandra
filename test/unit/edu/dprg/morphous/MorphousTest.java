package edu.dprg.morphous;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.cassandra.thrift.CqlResult;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MorphousTest extends CqlTestBase {
	private static Logger logger = LoggerFactory.getLogger(MorphousTest.class);
    
    @BeforeClass
    public static void setup() throws IOException {
    	startCassandra();
    }
    
    @Test
    public void testCreateAndDropTemporaryTable() {
    	String ksName = "TestKeyspace";
    	String cfName = "cf0";
    	executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		executeCql3Statement("CREATE TABLE " + ksName + "." + cfName + " ( col0 varchar PRIMARY KEY, col1 varchar);");
		
		CqlResult result = executeCql3Statement("SELECT * FROM " + ksName + "." + cfName);
		assertEquals(0, result.rows.size());
		
		for (int j = 0; j < 100; j++) {
        	executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName + " (col0, col1) VALUES ('cf0-col0-%03d', 'cf0-col1-%03d');", j, j));
        }
		
		result = executeCql3Statement("SELECT * FROM " + ksName + "." + cfName);
		assertEquals(100, result.rows.size());
		
		executeCql3Statement("DROP TABLE " + ksName + "." + cfName);
		
		try {
			result = executeCql3Statement("SELECT * FROM " + ksName + "." + cfName);	
		} catch (RuntimeException e) {
			return;
		}
		Assert.fail();
    }
    
    
}
