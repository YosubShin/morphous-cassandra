/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package edu.dprg.morphous;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.dprg.morphous.AtomicSwitchMorphousTaskHandler;
import edu.uiuc.dprg.morphous.Morphous;

@RunWith(OrderedJUnit4ClassRunner.class)
public class MoveSSTableTest extends CqlTestBase
{
	public static Logger logger = LoggerFactory.getLogger(MoveSSTableTest.class);
    
    @BeforeClass
    public static void setup() throws IOException {
    	startCassandra();
    }
	
	@Test
	public void testMoveSSTablesBetweenDifferentColumnFamilies() throws Exception {
	    final String ks1 = "testkeyspace_move_sstables";
	    final String cfName1 = "table1_copy";
	    final String cfName2 = "table1";
		
	  	List<KSMetaData> schema = new ArrayList<KSMetaData>();

        // A whole bucket of shorthand
        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;
        Map<String, String> opts_rf1 = KSMetaData.optsWithRF(1);

        schema.add(KSMetaData.testMetadata(ks1,  simple,  opts_rf1, 
        		new CFMetaData(ks1, cfName1, ColumnFamilyType.Standard, BytesType.instance, null),
        		new CFMetaData(ks1, cfName2, ColumnFamilyType.Standard, BytesType.instance, null)
        ));
        
        for (KSMetaData ksm : schema) {
        	MigrationManager.announceNewKeyspace(ksm);	
        }
		
		Keyspace keyspace = Keyspace.open(ks1);
		ColumnFamilyStore cfs1 = keyspace.getColumnFamilyStore(cfName1);
		cfs1.truncateBlocking();
		ColumnFamilyStore cfs2 = keyspace.getColumnFamilyStore(cfName2);
		
        cfs1.truncateBlocking();
        cfs2.truncateBlocking();
		
		for (int i = 1; i <= 500; i++) {
			ByteBuffer key = ByteBufferUtil.bytes("key-cf1-" + i);
			RowMutation rm = new RowMutation(ks1, key);
			rm.add(cfName1, ByteBufferUtil.bytes("Column1"), ByteBufferUtil.bytes("cf1-value" + i), 0);
			rm.apply();
		}
		cfs1.forceBlockingFlush();
		
		for (int i = 1; i <= 100; i++) {
			ByteBuffer key = ByteBufferUtil.bytes("key-cf2-" + i);
			RowMutation rm = new RowMutation(ks1, key);
			rm.add(cfName2, ByteBufferUtil.bytes("Column1"), ByteBufferUtil.bytes("cf2-value" + i), 0);
			rm.apply();
		}
		cfs2.forceBlockingFlush();
		
		SlicePredicate sp = new SlicePredicate();
        sp.setColumn_names(Arrays.asList(
            ByteBufferUtil.bytes("Column1")
        ));
        
		List<Row> rows1 = cfs1.getRangeSlice(Util.range("", ""),
                null,
                ThriftValidation.asIFilter(sp, cfs1.metadata, null),
                1000,
                System.currentTimeMillis(),
                true,
                false);
		assertEquals(500, rows1.size());
		
		List<Row> rows2 = cfs2.getRangeSlice(Util.range("", ""),
                null,
                ThriftValidation.asIFilter(sp, cfs2.metadata, null),
                1000,
                System.currentTimeMillis(),
                true,
                false);
		assertEquals(100, rows2.size());
		
		ColumnFamily cf = cfs1.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key-cf1-1"), cfName1, System.currentTimeMillis()));
		assertNotNull(cf);
		
		new AtomicSwitchMorphousTaskHandler().swapSSTablesBetweenCfs(cfs1, cfs2);
		
		cfs1.reload();
		rows1 = cfs1.getRangeSlice(Util.range("", ""),
                null,
                ThriftValidation.asIFilter(sp, cfs1.metadata, null),
                1000,
                System.currentTimeMillis(),
                true,
                false);
		assertEquals(100, rows1.size());		
		
		cfs2.reload();
		rows2 = cfs2.getRangeSlice(Util.range("", ""),
                null,
                ThriftValidation.asIFilter(sp, cfs2.metadata, null),
                1000,
                System.currentTimeMillis(),
                true,
                false);
		assertEquals(500, rows2.size());
	}
	
	@Test
	public void testMigrateColumnFamilyDefinitionToUseNewPartitonKey() throws Exception {
        String ksName = "testkeyspace_migrate_cf";
        String[] cfName = {"cf0", "cf1"};
		
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName[0] + " ( col0 varchar PRIMARY KEY, col1 varchar);");
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName[1] + " ( col0 varchar, col1 varchar PRIMARY KEY);");
		
        Keyspace ks = Keyspace.open(ksName);
        ColumnFamilyStore cfs0 = ks.getColumnFamilyStore(cfName[0]);
        ColumnFamilyStore cfs1 = ks.getColumnFamilyStore(cfName[1]);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 100; j++) {
            	edu.uiuc.dprg.morphous.Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName[i] + " (col0, col1) VALUES ('cf%d-col0-%03d', 'cf%d-col1-%03d');", i, j, i, j));
            }        	
        }
        
        CqlResult selectCf0 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[0] + ";");
        assertEquals(100, selectCf0.rows.size());
        
        CqlResult selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[1] + ";");
        assertEquals(100, selectCf1.rows.size());
        
        // Flush Memtables out to SSTables
        cfs0.forceBlockingFlush();
        cfs1.forceBlockingFlush();
        
        for (int i = 0; i < 100; i++) {
        	String query = "SELECT * FROM " + ksName + "." + cfName[0] + String.format(" WHERE col0 = 'cf0-col0-%03d';", i);
        	logger.info("Executing query {}", query);
        	selectCf0 = edu.uiuc.dprg.morphous.Util.executeCql3Statement(query);
        	assertEquals(1,  selectCf0.rows.size());
        }
        
        AtomicSwitchMorphousTaskHandler handler = new AtomicSwitchMorphousTaskHandler();
        handler.swapSSTablesBetweenCfs(cfs0, cfs1);
        Morphous.instance().migrateColumnFamilyDefinitionToUseNewPartitonKey(ksName, cfName[1], "col0");
        Morphous.instance().migrateColumnFamilyDefinitionToUseNewPartitonKey(ksName, cfName[0], "col1");
        
        cfs0.reload();
        cfs1.reload();
        
        selectCf0 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[0] + ";");
        assertEquals(100, selectCf0.rows.size());
        
        selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[1] + ";");
        assertEquals(100, selectCf1.rows.size());
        
        List<CqlRow> rows = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM system.schema_columns where keyspace_name = '" + ksName + "';").rows;
        
        for (int i = 0; i < 100; i++) {
        	String query = "SELECT * FROM " + ksName + "." + cfName[0] + String.format(" WHERE col1 = 'cf1-col1-%03d';", i);
        	logger.info("Executing query {}", query);
        	selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement(query);
        	assertEquals(1,  selectCf1.rows.size());
        }
        for (int i = 0; i < 100; i++) {
        	String query = "SELECT * FROM " + ksName + "." + cfName[1] + String.format(" WHERE col0 = 'cf0-col0-%03d';", i);
        	logger.info("Executing query {}", query);
        	selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement(query);
        	assertEquals(1,  selectCf1.rows.size());
        }
	}
	
	@Test
	public void testMigrateColumnFamilyDefinitionToUseNewPartitonKey2() throws Exception {
        String ksName = "testkeyspace_migrate_cf_2";
        String[] cfName = {"cf0", "cf1"};
		
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName[0] + " ( col0 int PRIMARY KEY, col1 int, col2 varchar);");
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName[1] + " ( col0 int, col1 int PRIMARY KEY, col2 varchar);");
		
        Keyspace ks = Keyspace.open(ksName);
        ColumnFamilyStore cfs0 = ks.getColumnFamilyStore(cfName[0]);
        ColumnFamilyStore cfs1 = ks.getColumnFamilyStore(cfName[1]);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 100; j++) {
            	edu.uiuc.dprg.morphous.Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName[i] + " (col0, col1, col2) VALUES (%d, %d, 'cf%d-col2-%03d');", j + i * 1000, 100 + j + i * 1000, i, j));
            }        	
        }
        
        CqlResult selectCf0 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[0] + ";");
        assertEquals(100, selectCf0.rows.size());
        
        CqlResult selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[1] + ";");
        assertEquals(100, selectCf1.rows.size());
        
        // Flush Memtables out to SSTables
        cfs0.forceBlockingFlush();
        cfs1.forceBlockingFlush();
        
        CqlResult originalResult;
        for (int i = 0; i < 100; i++) {
        	String query = "SELECT * FROM " + ksName + "." + cfName[0] + String.format(" WHERE col0 = %d;", i);
        	logger.info("Executing query {}", query);
        	originalResult = edu.uiuc.dprg.morphous.Util.executeCql3Statement(query);
        	assertEquals(1,  originalResult.rows.size());
        }
        
        originalResult = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[0] + ";");
        
        AtomicSwitchMorphousTaskHandler handler = new AtomicSwitchMorphousTaskHandler();
        handler.swapSSTablesBetweenCfs(cfs0, cfs1);
        Morphous.instance().migrateColumnFamilyDefinitionToUseNewPartitonKey(ksName, cfName[1], "col0");
        Morphous.instance().migrateColumnFamilyDefinitionToUseNewPartitonKey(ksName, cfName[0], "col1");
        
        cfs0.reload();
        cfs1.reload();
        
        selectCf0 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[0] + ";");
        assertEquals(100, selectCf0.rows.size());
        
        selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[1] + ";");
        assertEquals(100, selectCf1.rows.size());
        
        List<CqlRow> rows = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM system.schema_columns where keyspace_name = '" + ksName + "';").rows;
        
        for (int i = 0; i < 100; i++) {
        	String query = "SELECT * FROM " + ksName + "." + cfName[0] + String.format(" WHERE col1 = %d;", i + 100 + 1000);
        	logger.info("Executing query {}", query);
        	selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement(query);
        	assertEquals(1,  selectCf1.rows.size());
        }
        for (int i = 0; i < 100; i++) {
        	String query = "SELECT * FROM " + ksName + "." + cfName[1] + String.format(" WHERE col0 = %d;", i);
        	logger.info("Executing query {}", query);
        	selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement(query);
        	assertEquals(1,  selectCf1.rows.size());
        }
	}
	
	@Test
	public void testCreateTempTable() {
		String ksName = "testkeyspace_create_temp_table";
        String[] cfName = {"cf0", "cf1"};
		
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName[0] + " ( col0 varchar PRIMARY KEY, col1 varchar);");
		
        Keyspace ks = Keyspace.open(ksName);
        ColumnFamilyStore cfs0 = ks.getColumnFamilyStore(cfName[0]);
        
        CFMetaData newCfm = Morphous.instance().createNewCFMetaDataFromOldCFMetaDataWithNewCFNameAndNewPartitonKey(cfs0.metadata, cfName[1], "col1");
        Morphous.instance().createNewColumnFamilyWithCFMetaData(newCfm);
        
        ColumnFamilyStore cfs1 = ks.getColumnFamilyStore(cfName[1]);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 100; j++) {
            	edu.uiuc.dprg.morphous.Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName[i] + " (col0, col1) VALUES ('cf%d-col0-%03d', 'cf%d-col1-%03d');", i, j, i, j));
            }        	
        }
        
        CqlResult selectCf0 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[0] + ";");
        assertEquals(100, selectCf0.rows.size());
        
        CqlResult selectCf1 = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[1] + ";");
        assertEquals(100, selectCf1.rows.size());
        
        for (int j = 0; j < 100; j++) {
        	CqlResult selectCf = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[1] + " WHERE col1 = '" + String.format("cf1-col1-%03d", j) + "';");
        	assertEquals(1, selectCf.rows.size());
        }        
	}
}
