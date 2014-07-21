package edu.dprg.morphous;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.KeyBound;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.dprg.morphous.InsertMorphousTaskHandler;
import edu.uiuc.dprg.morphous.Util;

public class DataTransferTest extends CqlTestBase {
	private static Logger logger = LoggerFactory.getLogger(DataTransferTest.class);
    
    @BeforeClass
    public static void setup() throws IOException {
    	startCassandra();
    }
    
    @SuppressWarnings("rawtypes")
	@Test
    public void testScanLocalColumnFamily() throws Exception {
    	String ksName = "testscanlocal";
    	String cfName = "cf0";
    	Util.executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
		Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName + " ( col0 varchar PRIMARY KEY, col1 varchar);");
		
		Set<String> keySet = new HashSet<>();
		for (int j = 0; j < 1000; j++) {
			String key = String.format("cf0-col0-%03d", j);
			keySet.add(key);
        	Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName + " (col0, col1) VALUES ('cf0-col0-%03d', 'cf0-col1-%03d');", j, j));
        }
		
		CqlResult result = Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName);
		assertEquals(1000, result.rows.size());
		
		ColumnFamilyStore cfs = Keyspace.open(ksName).getColumnFamilyStore(cfName);

		TokenMetadata metadata = ((TokenMetadata) Util.getPrivateFieldWithReflection(StorageService.instance, "tokenMetadata")).cloneOnlyTokenMap();
		Collection<Range<KeyBound>> ranges = Arrays.asList(
				new Range<>(StorageService.getPartitioner().getMinimumToken().minKeyBound(), metadata.sortedTokens().get(0).maxKeyBound()),
				new Range<>(metadata.sortedTokens().get(0).maxKeyBound(), StorageService.getPartitioner().getMinimumToken().minKeyBound())
				);
		
		
		int count = 0;
		for (Range<?> range : ranges) {
			ColumnFamilyStore.AbstractScanIterator iterator = Util.invokePrivateMethodWithReflection(cfs, "getSequentialIterator", DataRange.forKeyRange((Range<Token>) range), System.currentTimeMillis());
    		
    		while (iterator.hasNext()) {			
    			Row row = iterator.next();
    			String keyString = ByteBufferUtil.string(row.key.key);
    			assertTrue(keySet.contains(keyString));
    			count++;
    		}
		}
		assertEquals(1000, count);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
  	@Test
    public void testInsertLocalRangesOnTemporaryCF() throws Exception {
      	String ksName = "testinsertlocal";
      	String cfName = "cf0";
      	Util.executeCql3Statement("CREATE KEYSPACE " + ksName + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
  		Util.executeCql3Statement("CREATE TABLE " + ksName + "." + cfName + " ( col0 varchar PRIMARY KEY, col1 varchar, col2_int int);");
  		
  		Set<String> keySet = new HashSet<>();
  		for (int j = 0; j < 1000; j++) {
  			String key = String.format("cf0-col0-%03d", j);
  			keySet.add(key);
          	Util.executeCql3Statement(String.format("INSERT INTO " + ksName + "." + cfName + " (col0, col1, col2_int) VALUES ('cf0-col0-%03d', 'cf0-col1-%03d', %d);", j, j, j));
          }
  		
  		CqlResult result = Util.executeCql3Statement("SELECT * FROM " + ksName + "." + cfName);
  		assertEquals(1000, result.rows.size());
  		
  		TokenMetadata metadata = ((TokenMetadata) Util.getPrivateFieldWithReflection(StorageService.instance, "tokenMetadata")).cloneOnlyTokenMap();
  		Collection<Range<Token>> ranges = new ArrayList<Range<Token>>();
  		ranges.add(new Range(StorageService.getPartitioner().getMinimumToken().minKeyBound(), metadata.sortedTokens().get(0).maxKeyBound()));
  		ranges.add(new Range(metadata.sortedTokens().get(0).maxKeyBound(), StorageService.getPartitioner().getMinimumToken().minKeyBound()));
  		
  		String tempCfName = "temp_" + cfName;
  		Util.executeCql3Statement("CREATE TABLE " + ksName + "." + tempCfName + " ( col0 varchar, col1 varchar PRIMARY KEY, col2_int int);");
  		
  		new InsertMorphousTaskHandler().insertLocalRangesOnTemporaryCF(ksName, cfName, tempCfName, "col1", ranges);
  		
  		result = Util.executeCql3Statement("SELECT * FROM " + ksName + "." + tempCfName);
  		assertEquals(1000, result.rows.size());
      }
    
    @Deprecated
    @SuppressWarnings("rawtypes")
	public static void doInsertOnTemporaryCFForRangesChoosingEndpointManually(String ksName, String originalCfName, String tempCfName, Collection<Range<Token>> ranges) {
    	for (Range<Token> range : ranges) {
    		ColumnFamilyStore originalCfs = Keyspace.open(ksName).getColumnFamilyStore(originalCfName);
    		ColumnFamilyStore tempCfs = Keyspace.open(ksName).getColumnFamilyStore(originalCfName);
    		ColumnFamilyStore.AbstractScanIterator iterator = Util.invokePrivateMethodWithReflection(originalCfs, "getSequentialIterator", DataRange.forKeyRange(range), System.currentTimeMillis());
    		
    		while (iterator.hasNext()) {
    			Row row = iterator.next();
    			ColumnFamily data = row.cf;
    			ColumnFamily tempData = TreeMapBackedSortedColumns.factory.create(ksName, tempCfName);
    			tempData.addAll(data, null);
    			
    			ByteBuffer newKey = tempData.getColumn(tempData.metadata().partitionKeyColumns().get(0).name).value();
    			InetAddress destinationNode = getDestinationNodeForKey(newKey);
    			
    			RowMutation rm = new RowMutation(newKey, tempData);
    			MessageOut<RowMutation> message = rm.createMessage();
    			MessagingService.instance().sendRR(message, destinationNode); //TODO Maybe use more robust way to send message
    		}
    	}
    }
    
    @Deprecated
    @SuppressWarnings("rawtypes")
	public static InetAddress getDestinationNodeForKey(ByteBuffer value) {
        try {
			logger.debug("Looking for new destination value for value : {}", ByteBufferUtil.string(value));
		} catch (CharacterCodingException e) {
			throw new RuntimeException(e);
		}
        // Mimics SimpleStrategy's implementation
        Token token = StorageService.getPartitioner().getToken(value);
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        ArrayList<Token> tokens = metadata.sortedTokens();
        Iterator<Token> iter = TokenMetadata.ringIterator(tokens, token, false);

        // Pick the node after the primary node.(Secondary node)
        Token primaryToken = iter.next();

        InetAddress endpoint = metadata.getEndpoint(primaryToken);
        logger.debug("Token : {}, sorted tokens : {}, primary Token : {}, endpoint : {}", token, tokens, primaryToken, endpoint);
        return endpoint;
    }
}
