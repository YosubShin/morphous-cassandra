package edu.uiuc.dprg.morphous;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTask;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponse;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponseStatus;

public class InsertMorphousTaskHandler implements MorphousTaskHandler {
	private static final Logger logger = LoggerFactory.getLogger(InsertMorphousTaskHandler.class);

	@SuppressWarnings("rawtypes")
	@Override
	public MorphousTaskResponse handle(MorphousTask task) {
		logger.debug("Handling Insert MorphouTask {}", task);
		MorphousTaskResponse response = new MorphousTaskResponse();
		response.status = MorphousTaskResponseStatus.SUCCESSFUL;
		response.taskUuid = task.taskUuid;
		
		ColumnFamilyStore originalCfs = Keyspace.open(task.keyspace).getColumnFamilyStore(task.columnFamily); 
		// Disable compaction on original table, since we only want to catch up the SSTables modified after this moment. 
		originalCfs.disableAutoCompaction();
		
		// TODO Change Insert Statement to RowMutation Message to single destination node
//		List<Range<Token>> ranges = (List<Range<Token>>) StorageService.instance.getPrimaryRangesForEndpoint(task.keyspace, FBUtilities.getBroadcastAddress());		
		Collection<Range<Token>> ranges = Util.getNthRangesForLocalNode(task.keyspace, 1);
		try {
			insertLocalRangesOnTemporaryCF(task.keyspace, task.columnFamily, Morphous.tempColumnFamilyName(task.columnFamily), task.newPartitionKey	, ranges);
		} catch (Exception e) {
			response.message = Throwables.getStackTraceAsString(e);
			response.status = MorphousTaskResponseStatus.FAILED;
		}
		return response;
	}

	@SuppressWarnings("rawtypes")
	public void insertLocalRangesOnTemporaryCF(String ksName, String originalCfName, String tempCfName, String newPartitionKey, Collection<Range<Token>> ranges) {
		int count = 0;
		ColumnFamilyStore originalCfs = Keyspace.open(ksName).getColumnFamilyStore(originalCfName);
		ByteBuffer oldPartitionKeyName = Util.getColumnNameByteBuffer(originalCfs.metadata.partitionKeyColumns().get(0).name.asReadOnlyBuffer());
		for (Range<Token> range : ranges) {
			ColumnFamilyStore.AbstractScanIterator iterator = Util.invokePrivateMethodWithReflection(originalCfs, "getSequentialIterator", DataRange.forKeyRange(range), System.currentTimeMillis());
			
			while (iterator.hasNext()) {
				Row row = iterator.next();
				ColumnFamily data = row.cf;
				ColumnFamily tempData = TreeMapBackedSortedColumns.factory.create(ksName, tempCfName);
				tempData.addAll(data, null);
				
				// Add column for original partition key because it's not present in old ColumnFamily
				tempData.addColumn(oldPartitionKeyName.asReadOnlyBuffer(), row.key.key, data.maxTimestamp());
				
				ByteBuffer newKey = tempData.getColumn(Util.getColumnNameByteBuffer(newPartitionKey)).value();
				
				RowMutation rm = new RowMutation(newKey, tempData);
				
				try {
					StorageProxy.mutateWithTriggers(Collections.singletonList(rm), ConsistencyLevel.QUORUM, false);
					count++;
				} catch (WriteTimeoutException | UnavailableException
						| OverloadedException | InvalidRequestException e) {
					throw new RuntimeException("Failed during inserting into temporary table.", e);
				}
			}
		}
		logger.info("Inserted {} rows into Keyspace {}, ColumnFamily {}", count, ksName, tempCfName);
	}
}
