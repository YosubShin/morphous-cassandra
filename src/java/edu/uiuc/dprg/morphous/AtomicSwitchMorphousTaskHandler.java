package edu.uiuc.dprg.morphous;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.CompactionManager;
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
            CompactionManager compactionManager = CompactionManager.instance;

            originalCfs.disableAutoCompaction();
            tempCfs.disableAutoCompaction();
            // Stop compactions that are being executed
            compactionManager.interruptCompactionFor(Arrays.asList(originalCfs.metadata, tempCfs.metadata), true);

			doBlockingFlushOnOriginalCFAndTempCF(originalCfs, tempCfs);
			swapSSTablesBetweenCfs(originalCfs, tempCfs);

			originalCfs.reload();
			tempCfs.reload();

            originalCfs.enableAutoCompaction();
            tempCfs.enableAutoCompaction();
		} finally {
			// Unblock local writes (does not affect other nodes in the cluster)
			logger.info("Unlocking write lock for keyspace {}, column family {} since swapping of the table is over", task.keyspace, task.columnFamily);
			QueryProcessor.processInternal(String.format("update system.morphous_status set swapping = False where keyspace_name = '%s' and columnfamily_name = '%s';", task.keyspace, task.columnFamily));
		}

		logger.debug("AtomcSwitchMorphousTask {} finished in {} ms, and generated response : {}", task, System.currentTimeMillis() - startAt, response);
		return response;
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

        List<String> componentList = new ArrayList<>();
        for (Set<Component> components : leftSstableList.values()) {
            for (Component component: components) {
                componentList.add(component.name());
            }
        }
        logger.debug("Components for left dir: {}", Arrays.toString(componentList.toArray()));

        componentList = new ArrayList<>();
        for (Set<Component> components : rightSstableList.values()) {
            for (Component component: components) {
                componentList.add(component.name());
            }
        }
        logger.debug("Components for right dir: {}", Arrays.toString(componentList.toArray()));


        logger.debug("LeftDir:{}", Arrays.toString(leftSstableList.keySet().iterator().next().directory.list()));
        logger.debug("RightDir:{}", Arrays.toString(rightSstableList.keySet().iterator().next().directory.list()));

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
				logger.debug("Moving SSTable File {} to {}", srcDescriptor.filenameFor(component), destDescriptor.filenameFor(component));

//				FileUtils.renameWithOutConfirm(srcDescriptor.filenameFor(component), destDescriptor.filenameFor(component));
                try {
                    FileUtils.renameWithConfirm(srcDescriptor.filenameFor(component), destDescriptor.filenameFor(component));
                } catch (AssertionError e) {
                    logger.error("AssertionError {}", e);
                    logger.error("From directory: {}", srcDescriptor.directory.list());
                    logger.error("To directory: {}", destDescriptor.directory.list());
                }
			}
			
			try {
				destNewSSTables.add(SSTableReader.open(destDescriptor));
			} catch (IOException e)
			{
				SSTableReader.logOpenException(entry.getKey(), e);
				continue;
			}
		}
		return destNewSSTables;
	}
	
}
