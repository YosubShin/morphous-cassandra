package edu.uiuc.dprg.morphous;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterables;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTask;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponse;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponseStatus;

public class InsertMorphousTaskHandler implements MorphousTaskHandler {
	private static final Logger logger = LoggerFactory.getLogger(InsertMorphousTaskHandler.class);
	private static final long throttleMillis = 0;

	@SuppressWarnings("rawtypes")
	@Override
	public MorphousTaskResponse handle(MorphousTask task) {
		logger.debug("Handling Insert MorphouTask {}", task);
		MorphousTaskResponse response = new MorphousTaskResponse();
		response.status = MorphousTaskResponseStatus.SUCCESSFUL;
		response.taskUuid = task.taskUuid;

		ColumnFamilyStore originalCfs = Keyspace.open(task.keyspace).getColumnFamilyStore(task.columnFamily);
        ColumnFamilyStore tempCfs = Keyspace.open(task.keyspace).getColumnFamilyStore(Morphous.tempColumnFamilyName(task.columnFamily));

        // In case CompactPhase does not exist, we need to disable automatic compactions during reconfiguration
        originalCfs.disableAutoCompaction();
        tempCfs.disableAutoCompaction();

        // Here, we are assuming that the cluster has gone through the major compaction
		Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(task.keyspace);
		try {
			insertLocalRangesOnTemporaryCF(task.keyspace, task.columnFamily, Morphous.tempColumnFamilyName(task.columnFamily), task.newPartitionKey	, ranges);
		} catch (Exception e) {
			logger.error("Error while inserting local ranges", e);
			response.message = Throwables.getStackTraceAsString(e);
			response.status = MorphousTaskResponseStatus.FAILED;
		}
		return response;
	}

	@SuppressWarnings("rawtypes")
	public void insertLocalRangesOnTemporaryCF(String ksName, String originalCfName, String tempCfName, String newPartitionKey, Collection<Range<Token>> ranges) {
		int count = 0;
		final ColumnFamilyStore originalCfs = Keyspace.open(ksName).getColumnFamilyStore(originalCfName);
		ByteBuffer oldPartitionKeyName = Util.getColumnNameByteBuffer(originalCfs.metadata.partitionKeyColumns().get(0).name.asReadOnlyBuffer());
		for (final Range<Token> range : ranges) {
			logger.debug("Getting AbstractScanIterator for local Range {} for insertion", range);
            logger.debug("Is wraparound={}", range.isWrapAround());
            Iterator<Row> iterator;
            if (range.isWrapAround()) {
                final List<Range<Token>> unwrappedRanges = range.unwrap();
                iterator = Iterables.concat(
                        new Iterable<Row>() {
                            @Override
                            public Iterator<Row> iterator() {
                                return Util.invokePrivateMethodWithReflection(originalCfs, "getSequentialIterator", DataRange.forKeyRange(unwrappedRanges.get(0)), System.currentTimeMillis());
                            }
                        },
                        new Iterable<Row>() {
                            @Override
                            public Iterator<Row> iterator() {
                                return Util.invokePrivateMethodWithReflection(originalCfs, "getSequentialIterator", DataRange.forKeyRange(unwrappedRanges.get(1)), System.currentTimeMillis());
                            }
                        }).iterator();
            } else {
                iterator = Util.invokePrivateMethodWithReflection(originalCfs, "getSequentialIterator", DataRange.forKeyRange(range), System.currentTimeMillis());
            }

			while (iterator.hasNext()) {
				Row row = iterator.next();
				ColumnFamily data = row.cf;
				ColumnFamily tempData = TreeMapBackedSortedColumns.factory.create(ksName, tempCfName);
				tempData.addAll(data, null);
				
				// Add column for original partition key because it's not present in old ColumnFamily
				tempData.addColumn(oldPartitionKeyName.asReadOnlyBuffer(), row.key.key, data.maxTimestamp());

				Column newPartitionKeyColumn = tempData.getColumn(Util.getColumnNameByteBuffer(newPartitionKey));
				if (newPartitionKeyColumn == null) {
					logger.warn("No new partition key column value exists for row {}", row);
					continue;
				}
				ByteBuffer newKey = newPartitionKeyColumn.value();
				
				RowMutation rm = new RowMutation(newKey, tempData);
				
				int destinationReplicaIndex =  edu.uiuc.dprg.morphous.Util.getReplicaIndexForKey(ksName, row.key.key);
				Morphous.sendRowMutationToNthReplicaNode(rm, destinationReplicaIndex + 1);
                count++;
				try {
					if (throttleMillis > 0) {
						Thread.sleep(throttleMillis);
					}
				} catch (InterruptedException e) {
					throw new MorphousException("Exception while sleeping for throttling", e);
				}
			}
		}
		logger.info("Inserted {} rows into Keyspace {}, ColumnFamily {}", count, ksName, tempCfName);
	}
}
