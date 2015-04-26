package edu.uiuc.dprg.morphous;

import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTask;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponse;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponseStatus;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class CatchupMorphousTaskHandler implements MorphousTaskHandler {
	private static final Logger logger = LoggerFactory.getLogger(CatchupMorphousTaskHandler.class);

	@Override
	public MorphousTaskResponse handle(MorphousTask task) {
		long startAt = System.currentTimeMillis();
		logger.debug("Handling Catchup MorphouTask {}", task);
		MorphousTaskResponse response = new MorphousTaskResponse();
		response.status = MorphousTaskResponseStatus.SUCCESSFUL;
		response.taskUuid = task.taskUuid;

		Keyspace keyspace = Keyspace.open(task.keyspace);
		ColumnFamilyStore originalCfs = keyspace.getColumnFamilyStore(task.columnFamily);
		ColumnFamilyStore tempCfs = keyspace.getColumnFamilyStore(Morphous.tempColumnFamilyName(task.columnFamily));

        // Trigger catch up
        replayRecentSstablesAtTempColumnFamily(originalCfs, tempCfs, task.taskStartedAtInMicro, response);

		logger.debug("CatchupMorphousTask {} finished in {} ms, and generated response : {}", task, System.currentTimeMillis() - startAt, response);
		return response;
	}

	public void replayRecentSstablesAtTempColumnFamily(ColumnFamilyStore originalCfs, ColumnFamilyStore tempCfs, long replayAfterInMicro, MorphousTaskResponse response) {
		logger.debug("Replaying recent updates in the SSTables since reconfiguration has started at {}us, in column family {}", replayAfterInMicro, originalCfs.name);
		int sstableCount = 0;
		// Filter only SSTables that's newer than replayAfter value
		List<SSTableReader> sstables = new ArrayList<SSTableReader>();
		for (SSTableReader sstable : tempCfs.getSSTables()) {
			if (sstable.getMaxTimestamp() > replayAfterInMicro) {
				sstables.add(sstable);
				sstableCount++;
			}
		}
		// Sort SSTableReaders by chronological order
		Collections.sort(sstables, SSTable.maxTimestampComparator);

        int totalRowCount = 0;
		int successfulRowCount = 0;
		int partialUpdateFailureCount = 0;
		int destinationReplicaIndexFindFailureCount = 0;

		for (SSTableReader sstable : sstables) {
			SSTableScanner scanner = sstable.getScanner();
			while (scanner.hasNext()) {
				OnDiskAtomIterator onDiskAtomIterator = scanner.next();
				DecoratedKey tempKey = onDiskAtomIterator.getKey();
				ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(originalCfs.metadata);
				// Add partition key Column of Temp table into the newly created ColumnFamily
				// Set the timestamp to be the time when reconfiguration has started. (the timestamp has to be in millisecond in this case)
				cf.addColumn(new Column(Util.getColumnNameByteBuffer(Morphous.getPartitionKeyNameByteBuffer(tempCfs)), ((ByteBuffer) tempKey.key.rewind()).asReadOnlyBuffer(), replayAfterInMicro / 1000));
				while (onDiskAtomIterator.hasNext()) {
					// This should be enough to preserve timestamp, because I'm not touching anything from original columns.
					cf.addAtom(onDiskAtomIterator.next());
				}

				RowMutation rm = null;
				try {
                    totalRowCount++;
					rm = new RowMutation(Util.getKeyByteBufferForCf(cf), cf);
				} catch (PartialUpdateException e) {
//					logger.warn("Partial update is currently not supported", e);
					partialUpdateFailureCount++;
//					continue;
					String originalCfPkName = Morphous.getPartitionKeyName(originalCfs);
					String tempCfPkName = Morphous.getPartitionKeyName(tempCfs);

					String query = String.format("SELECT %s FROM %s WHERE %s = '%s';", originalCfPkName, tempCfs.name, tempCfPkName, Util.toStringByteBuffer((ByteBuffer) tempKey.key.rewind()));
					try {
						UntypedResultSet result = QueryProcessor.process(query, ConsistencyLevel.ONE);
						Iterator<UntypedResultSet.Row> iter = result.iterator();
						while (iter.hasNext()) {
							UntypedResultSet.Row row = iter.next();
							ByteBuffer key = row.getBytes(originalCfPkName);
							if (key != null) {
								rm = new RowMutation(key, cf);
							} else {
								logger.warn("No new Partition key column available in temp table either");
                                throw new MorphousException("No new Partition key column available in temp table either");
							}
						}
					} catch (RequestExecutionException | MorphousException e1) {
//						throw new MorphousException("Failed to fall back for PartialUpdate", e1);
                        logger.warn("Failed to fall back for PartialUpdate", e1);
                        partialUpdateFailureCount++;
                        continue;
					}
				}

				int destinationReplicaIndex;
				try {
					destinationReplicaIndex = Util.getReplicaIndexForKey(tempCfs.keyspace.getName(), tempKey.key);
				} catch (MorphousException e) {
					logger.error("error in getReplicasIndexForKey: cf={}, key={}");
					destinationReplicaIndexFindFailureCount++;
					continue;
				}

                if (rm != null) {
				    Morphous.sendRowMutationToNthReplicaNode(rm, destinationReplicaIndex + 1);
                    successfulRowCount++;
                }
			}
		}
		logger.info("Replayed for # of sstables={}, # of rows={}", sstableCount, successfulRowCount);
		response.message = String.format("Replayed for # of sstables=%d. Out of total # of rows=%d, successfully updated # of rows=%d, # of failed partial updates=%d, # of failed destinationReplicas lookup=%d",
				sstableCount, totalRowCount, successfulRowCount, partialUpdateFailureCount, destinationReplicaIndexFindFailureCount);
	}
}
