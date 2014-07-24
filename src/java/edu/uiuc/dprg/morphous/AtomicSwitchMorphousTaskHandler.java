package edu.uiuc.dprg.morphous;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
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
			// TODO Block all local write requests
			
			doBlockingFlushOnOriginalCFAndTempCF(originalCfs, tempCfs);
			swapSSTablesBetweenCfs(originalCfs, tempCfs);
			
			originalCfs.reload();
			tempCfs.reload();
			
			// Trigger more catch up on last minute changes
			
		} finally {
			// TODO Unblock local writes
		}
		
		logger.debug("AtomcSwitchMorphousTask {} finished in {} ms, and generated response : {}", task, System.currentTimeMillis() - startAt, response);
		return response;
	}
	
	
	public void doBlockingFlushOnOriginalCFAndTempCF(ColumnFamilyStore originalCfs, ColumnFamilyStore tempCfs) {
		Future<?>[] futures = {
				originalCfs.forceFlush(), 
				tempCfs.forceFlush()
				};
		long timeout = 10000;
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