package edu.dprg.morphous;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.cassandra.thrift.CqlResult;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.dprg.morphous.Util;

public class MorphousTest extends CqlTestBase {
	private static Logger logger = LoggerFactory.getLogger(MorphousTest.class);
    
    @BeforeClass
    public static void setup() throws IOException {
    	startCassandra();
    }
    
    @Test
    public void testCreateAndDropTemporaryTable() {
    	String ksName = "testeyspace_create_and_drop";
    	String cfName = "cf0";
    	Util.executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName + " ( col0 varchar PRIMARY KEY, col1 varchar);");
		
		CqlResult result = Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName);
		assertEquals(0, result.rows.size());
		
		for (int j = 0; j < 100; j++) {
        	Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName + " (col0, col1) VALUES ('cf0-col0-%03d', 'cf0-col1-%03d');", j, j));
        }
		
		result = Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName);
		assertEquals(100, result.rows.size());
		
		Util.executeCql3Statement("DROP TABLE " + ksName + "." + cfName);
		
		try {
			result = Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName);	
		} catch (RuntimeException e) {
			return;
		}
		Assert.fail();
    }
    
    
}
