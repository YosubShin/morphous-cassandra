package edu.dprg.morphous;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.thrift.CqlResult;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.dprg.morphous.Morphous;

@RunWith(OrderedJUnit4ClassRunner.class)
public class SSTableCatchupTest extends CqlTestBase {

	public static Logger logger = LoggerFactory.getLogger(SSTableCatchupTest.class);
    
    @BeforeClass
    public static void setup() throws IOException {
    	startCassandra();
    }
    

	@Test
	public void testGetRecentSSTables() {
		String ksName = "testkeyspace_get_recent_sstables";
		String[] cfName = {"cf0"};
		
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName[0] + " ( col0 varchar PRIMARY KEY, col1 varchar);");
		
        Keyspace ks = Keyspace.open(ksName);
        ColumnFamilyStore cfs0 = ks.getColumnFamilyStore(cfName[0]);
        
        long start = System.currentTimeMillis();
        
        logger.info("SSTable count = {}", cfs0.getSSTables().size());
        
        cfs0.disableAutoCompaction();
        
        for (int i = 0; i < 5; i++) {
        	for (int j = i * 100; j < i * 100 + 100; j++) {
            	edu.uiuc.dprg.morphous.Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName[0] + " (col0, col1) VALUES ('cf%d-col0-%03d', 'cf%d-col1-%03d');", 0, j, 0, j));
            }        	
            cfs0.forceBlockingFlush();	
            logger.info("SSTable count = {}", cfs0.getSSTables().size());
        }
        
        
        long between = System.currentTimeMillis();
        
        for (int i = 5; i < 10; i++) {
        	for (int j = i * 100; j < i * 100 + 100; j++) {
            	edu.uiuc.dprg.morphous.Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName[0] + " (col0, col1) VALUES ('cf%d-col0-%03d', 'cf%d-col1-%03d');", 0, j, 0, j));
            }        	
            cfs0.forceBlockingFlush();	
            logger.info("SSTable count = {}", cfs0.getSSTables().size());
        }
        
        long end = System.currentTimeMillis();
        
        Collection<SSTableReader> sstables = cfs0.getSSTables();
        Collection<SSTableReader> firstHalfSstables = new ArrayList<SSTableReader>();
        Collection<SSTableReader> secondHalfSstables = new ArrayList<SSTableReader>();
        
        for (SSTableReader sstable : sstables) {
        	long maxTimestamp = sstable.getMaxTimestamp() / 1000;
        	logger.info("max timestamp = {}  where now is {}, for sstable = {}", maxTimestamp, System.currentTimeMillis(), sstable);
        	if (maxTimestamp > start && maxTimestamp < between) {
        		firstHalfSstables.add(sstable);
        	} else if (maxTimestamp > between && maxTimestamp < end) {
        		secondHalfSstables.add(sstable);
        	} else {
        		Assert.fail("SSTableReader " + sstable + " does not fall into possible timestamp range");
        	}
        }
        
        logger.info("there are {} sstables", sstables.size());
        
        assertEquals(5, firstHalfSstables.size());
        assertEquals(5, secondHalfSstables.size());
        
	}
	
	
	@Test
	public void testScanSingleSSTable() {
		String ksName = "testkeyspace_scan_single_sstable";
		String[] cfName = {"cf0", "cf1"};
		
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		edu.uiuc.dprg.morphous.Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName[0] + " ( col0 varchar PRIMARY KEY, col1 varchar);");
		
        Keyspace ks = Keyspace.open(ksName);
        ColumnFamilyStore cfs0 = ks.getColumnFamilyStore(cfName[0]);
        
        CFMetaData newCfm = Morphous.instance().createNewCFMetaDataFromOldCFMetaDataWithNewCFNameAndNewPartitonKey(cfs0.metadata, cfName[1], "col1");
        Morphous.instance().createNewColumnFamilyWithCFMetaData(newCfm);
                
        logger.info("SSTable count = {}", cfs0.getSSTables().size());
        
        cfs0.disableAutoCompaction();
        
        for (int j = 0; j < 100; j++) {
        	edu.uiuc.dprg.morphous.Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName[0] + " (col0, col1) VALUES ('cf%d-col0-%03d', 'cf%d-col1-%03d');", 0, j, 0, j));
        }        	
        cfs0.forceBlockingFlush();
        
        SSTableReader sstable = cfs0.getSSTables().iterator().next();
        
        SSTableScanner sstableScanner = sstable.getScanner();
        while (sstableScanner.hasNext()) {
        	OnDiskAtomIterator onDiskAtomIterator = sstableScanner.next();
        	DecoratedKey originalKey = onDiskAtomIterator.getKey();
        	ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(newCfm);
			cf.addColumn(new Column(edu.uiuc.dprg.morphous.Util.getColumnNameByteBuffer(Morphous.getPartitionKeyNameByteBuffer(cfs0)), ((ByteBuffer) originalKey.key.rewind()).asReadOnlyBuffer()));
        	
        	while (onDiskAtomIterator.hasNext()) {
        		OnDiskAtom atom = onDiskAtomIterator.next();
        		cf.addAtom(atom);
        		Column column = (Column) atom;
        		logger.info("Column key : {}, value : {}", edu.uiuc.dprg.morphous.Util.toStringByteBuffer(column.name(), String.class), edu.uiuc.dprg.morphous.Util.toStringByteBuffer(column.value(), String.class));
        	}
        	logger.info("key : {}, cf : {}", edu.uiuc.dprg.morphous.Util.toStringByteBuffer(originalKey.key, String.class), cf);
        	
        	RowMutation rm = new RowMutation(edu.uiuc.dprg.morphous.Util.getKeyByteBufferForCf(cf), cf);
        	
        	Morphous.instance().sendRowMutationToNthReplicaNode(rm, edu.uiuc.dprg.morphous.Util.getReplicaIndexForKey(ksName, originalKey.key) + 1);
        }
        
        for (int j = 0; j < 100; j++) {
        	CqlResult selectCf = edu.uiuc.dprg.morphous.Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName[1] + " WHERE col1 = '" + String.format("cf0-col1-%03d", j) + "';");
        	assertEquals(1, selectCf.rows.size());
        }
	}
}
