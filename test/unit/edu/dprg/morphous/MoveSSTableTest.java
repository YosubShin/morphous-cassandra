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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(OrderedJUnit4ClassRunner.class)
public class MoveSSTableTest
{
	private static Logger logger = LoggerFactory.getLogger(SchemaLoader.class);
    private static final String ks1 = "Keyspace1";
    private static final String cfName1 = "table1_copy";
    private static final String cfName2 = "table1";
	
	@BeforeClass
	public static void loadSchema() throws Exception {
		SchemaLoader.cleanupAndLeaveDirs();
		
		Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
		{
			public void uncaughtException(Thread t, Throwable e)
			{
				logger.error("Fatal exception in thread " + t, e);
			}
		});
	  	SchemaLoader.startGossiper();
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
	}
	
	@AfterClass
	public static void stopGossiper() {
		Gossiper.instance.stop();
	}
	
	@Test
	public void testMoveSSTablesBetweenDifferentColumnFamilies() throws Exception {
		Keyspace keyspace = Keyspace.open(ks1);
		ColumnFamilyStore cfs1 = keyspace.getColumnFamilyStore(cfName1);
		cfs1.truncateBlocking();
		ColumnFamilyStore cfs2 = keyspace.getColumnFamilyStore(cfName2);
		
        cfs1.truncateBlocking();
        cfs2.truncateBlocking();
		
		for (int i = 1; i <= 100; i++) {
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
		assertEquals(100, rows1.size());
		
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
		
		moveSSTablesFromDifferentCF(keyspace, cfs1, cfs2);
		
		cfs1.reload();
		rows1 = cfs1.getRangeSlice(Util.range("", ""),
                null,
                ThriftValidation.asIFilter(sp, cfs1.metadata, null),
                1000,
                System.currentTimeMillis(),
                true,
                false);
		assertEquals(0, rows1.size());		
		
		cfs2.reload();
		rows2 = cfs2.getRangeSlice(Util.range("", ""),
                null,
                ThriftValidation.asIFilter(sp, cfs2.metadata, null),
                1000,
                System.currentTimeMillis(),
                true,
                false);
		assertEquals(100, rows2.size());
		logger.info("Testing logger functionality");
	}

	private void moveSSTablesFromDifferentCF(Keyspace keyspace, ColumnFamilyStore from,
			ColumnFamilyStore to) {
		Directories originalDirectories = Directories.create(keyspace.getName(), from.name);
		Directories destDirectory = Directories.create(keyspace.getName(), to.name);
		
		Collection<SSTableReader> destNewSSTables = new HashSet<>();
		
		for (Entry<Descriptor, Set<Component>> entry : originalDirectories.sstableLister().list().entrySet()) {
			Descriptor srcDescriptor = entry.getKey();
			Descriptor destDescriptor = new Descriptor(
					destDirectory.getDirectoryForNewSSTables(),
					keyspace.getName(),
					to.name,
					((AtomicInteger) edu.dprg.morphous.Util.getPrivateFieldWithReflection(to, "fileIndexGenerator")).incrementAndGet(), 
					false);
			logger.debug("Moving SSTable {} to {}", srcDescriptor.directory, destDescriptor.directory);
			for (Component component : entry.getValue()) {
				FileUtils.renameWithConfirm(srcDescriptor.filenameFor(component), destDescriptor.filenameFor(component));
			}
			
			try {
				destNewSSTables.add(SSTableReader.open(destDescriptor));
			} catch (IOException e) {
				logger.error("Exception while creating a new SSTableReader {}", e);
				throw new RuntimeException(e);
			}
		}
		
		// Remove SSTable from memory in temporary CF
		for (File directory : originalDirectories.getCFDirectories()) {
			edu.dprg.morphous.Util.invokePrivateMethodWithReflection(from.getDataTracker(), "removeUnreadableSSTables", directory);
		}
		
		// Add copied SSTable to destination CF, and remove old SSTables from destination CF
		Set<SSTableReader> destOldSSTables = to.getDataTracker().getSSTables();
		to.getDataTracker().replaceCompactedSSTables(destOldSSTables, destNewSSTables, OperationType.UNKNOWN);
		
	}
}
