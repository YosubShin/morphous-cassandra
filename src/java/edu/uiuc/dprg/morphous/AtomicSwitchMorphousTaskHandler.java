package edu.uiuc.dprg.morphous;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTask;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponse;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponseStatus;

public class AtomicSwitchMorphousTaskHandler implements MorphousTaskHandler {
	private static final Logger logger = LoggerFactory.getLogger(AtomicSwitchMorphousTaskHandler.class);

	@Override
	public MorphousTaskResponse handle(MorphousTask task) {
		long startAt = System.currentTimeMillis();
		logger.debug("Handling Atomic Switch MorphouTask {}", task);
		MorphousTaskResponse response = new MorphousTaskResponse();
		response.status = MorphousTaskResponseStatus.SUCCESSFUL;
		response.taskUuid = task.taskUuid;
		
		Keyspace keyspace = Keyspace.open(task.keyspace);
		ColumnFamilyStore originalCfs = keyspace.getColumnFamilyStore(task.columnFamily);
		ColumnFamilyStore tempCfs = keyspace.getColumnFamilyStore(Morphous.tempColumnFamilyName(task.columnFamily));
		
		try {
			// All write requests on this Column family is being blocked now
			doBlockingFlushOnOriginalCFAndTempCF(originalCfs, tempCfs);
			swapSSTablesBetweenCfs(originalCfs, tempCfs);
			
			originalCfs.reload();
			tempCfs.reload();
		} finally {
            // Unblock local writes (does not affect other nodes in the cluster)
            logger.info("Unlocking write lock for keyspace {}, column family {} since swapping of the table is over", task.keyspace, task.columnFamily);
            QueryProcessor.processInternal(String.format("update system.morphous_status set swapping = False where keyspace_name = '%s' and columnfamily_name = '%s';", task.keyspace, task.columnFamily));
        }
        // Trigger catch up
        replayRecentSstablesAtTempColumnFamily(originalCfs, tempCfs, task.taskStartedAtInMicro);

		logger.debug("AtomcSwitchMorphousTask {} finished in {} ms, and generated response : {}", task, System.currentTimeMillis() - startAt, response);
		return response;
	}
	
	public void replayRecentSstablesAtTempColumnFamily(ColumnFamilyStore originalCfs, ColumnFamilyStore tempCfs, long replayAfterInMicro) {
        logger.debug("Replaying recent updates in the SSTables since reconfiguration has started at {}us, in column family {}", replayAfterInMicro, originalCfs.name);
		// Filter only SSTables that's newer than replayAfter value
		List<SSTableReader> sstables = new ArrayList<SSTableReader>();
		for (SSTableReader sstable : tempCfs.getSSTables()) {
			if (sstable.getMaxTimestamp() > replayAfterInMicro) {
				sstables.add(sstable);
			}
		}
		// Sort SSTableReaders by chronological order
		Collections.sort(sstables, SSTable.maxTimestampComparator);
		
		for (SSTableReader sstable : sstables) {
			SSTableScanner scanner = sstable.getScanner();
			while (scanner.hasNext()) {
				OnDiskAtomIterator onDiskAtomIterator = scanner.next();
				DecoratedKey tempKey = onDiskAtomIterator.getKey();
	        	ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(originalCfs.metadata);
	        	// Add partition key Column of Temp table into the newly created ColumnFamily
                // Set the timestamp to be the time when reconfiguration has started. (the timestamp has to be in millisecond in this case)
	        	cf.addColumn(new Column(edu.uiuc.dprg.morphous.Util.getColumnNameByteBuffer(Morphous.getPartitionKeyNameByteBuffer(tempCfs)), ((ByteBuffer) tempKey.key.rewind()).asReadOnlyBuffer(), replayAfterInMicro / 1000));
	        	while (onDiskAtomIterator.hasNext()) {
                    // This should be enough to preserve timestamp, because I'm not touching anything from original columns.
	        		cf.addAtom(onDiskAtomIterator.next());
	        	}

	        	RowMutation rm;
                try {
                    rm = new RowMutation(edu.uiuc.dprg.morphous.Util.getKeyByteBufferForCf(cf), cf);
                } catch (PartialUpdateException e) {
					logger.warn("Partial update is currently not supported", e);
					continue;
//					String originalCfPkName = Morphous.getPartitionKeyName(originalCfs);
//					String tempCfPkName = Morphous.getPartitionKeyName(tempCfs);
//
//					String query = String.format("SELECT %s FROM %s WHERE %s = '%s';", originalCfPkName, tempCfs.name, tempCfPkName, Util.toStringByteBuffer((ByteBuffer) tempKey.key.rewind()));
//					try {
//						UntypedResultSet result = QueryProcessor.process(query, ConsistencyLevel.ONE);
//						Iterator<UntypedResultSet.Row> iter = result.iterator();
//						while (iter.hasNext()) {
//							UntypedResultSet.Row row = iter.next();
//							ByteBuffer key = row.getBytes(originalCfPkName);
//							if (key != null) {
//								rm = new RowMutation(key, cf);
//							} else {
//								logger.warn("No new Partition key column available in temp table either");
//							}
//						}
//					} catch (RequestExecutionException e1) {
//						throw new MorphousException("Failed to fall back for PartialUpdate", e1);
//					}
                }

	        	int destinationReplicaIndex =  edu.uiuc.dprg.morphous.Util.getReplicaIndexForKey(tempCfs.keyspace.getName(), tempKey.key);
	        	Morphous.sendRowMutationToNthReplicaNode(rm, destinationReplicaIndex + 1);
			}
		}
		
	}
	
	
	public void doBlockingFlushOnOriginalCFAndTempCF(ColumnFamilyStore originalCfs, ColumnFamilyStore tempCfs) {
		Future<?>[] futures = {
				originalCfs.forceFlush(), 
				tempCfs.forceFlush()
				};
		long timeout = 30000;
		long startAt = System.currentTimeMillis();
		outer_while_loop:
		while (System.currentTimeMillis() - startAt < timeout) {
			for (Future<?> future : futures) {
				if (!future.isDone()) {
					continue outer_while_loop;
				}
			}
			// Flushes on both CFs are done!
			return;
		}
		throw new MorphousException("Flushes on CFs timed out!");
	}

	public void swapSSTablesBetweenCfs(ColumnFamilyStore left, ColumnFamilyStore right) {
		logger.debug("Swapping SSTables files between {} and {}", left, right);
		Keyspace keyspace = left.keyspace;
		Directories leftDirs = Directories.create(keyspace.getName(), left.name);
		Directories rightDirs = Directories.create(keyspace.getName(), right.name);
		
		Map<Descriptor, Set<Component>> leftSstableList = leftDirs.sstableLister().list();
		Map<Descriptor, Set<Component>> rightSstableList = rightDirs.sstableLister().list();
		
		Collection<SSTableReader> movedSstablesFromLeftToRight = movePhysicalSSTableFiles(leftSstableList, right);
		Collection<SSTableReader> movedSstablesFromRightToLeft = movePhysicalSSTableFiles(rightSstableList, left);
		
		replaceSstablesInMemory(movedSstablesFromLeftToRight, right);
		replaceSstablesInMemory(movedSstablesFromRightToLeft, left);
	}
	
	public void replaceSstablesInMemory(Collection<SSTableReader> newSstables, ColumnFamilyStore cfs) {
		Set<SSTableReader> oldSstables = cfs.getDataTracker().getSSTables();
		cfs.getDataTracker().replaceCompactedSSTables(oldSstables, newSstables, OperationType.UNKNOWN);
	}
	

	public Collection<SSTableReader> movePhysicalSSTableFiles(Map<Descriptor, Set<Component>> srcSstableList, ColumnFamilyStore destCfs) {
		Keyspace keyspace = destCfs.keyspace;
		Directories destDirectory = Directories.create(keyspace.getName(), destCfs.name);
		Collection<SSTableReader> destNewSSTables = new HashSet<>();
		
		for (Entry<Descriptor, Set<Component>> entry : srcSstableList.entrySet()) {
			Descriptor srcDescriptor = entry.getKey();
			Descriptor destDescriptor = new Descriptor(
					destDirectory.getDirectoryForNewSSTables(),
					keyspace.getName(),
					destCfs.name,
					((AtomicInteger) edu.uiuc.dprg.morphous.Util.getPrivateFieldWithReflection(destCfs, "fileIndexGenerator")).incrementAndGet(), 
					false);
			for (Component component : entry.getValue()) {
				logger.debug("Moving SSTable File {} from {} to {}", srcDescriptor.filenameFor(component), srcDescriptor.directory, destDescriptor.directory);
				FileUtils.renameWithConfirm(srcDescriptor.filenameFor(component), destDescriptor.filenameFor(component));
			}
			
			try {
				destNewSSTables.add(SSTableReader.open(destDescriptor));
			} catch (IOException e) {
				throw new MorphousException("Error while creating a new SSTableReader", e);
			}
		}
		return destNewSSTables;
	}
	
}
