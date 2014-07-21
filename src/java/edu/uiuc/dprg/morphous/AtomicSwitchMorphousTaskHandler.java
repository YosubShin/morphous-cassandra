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

public class AtomicSwitchMorphousTaskHandler implements MorphousTaskHandler {
	private static final Logger logger = LoggerFactory.getLogger(AtomicSwitchMorphousTaskHandler.class);

	@SuppressWarnings("rawtypes")
	@Override
	public MorphousTaskResponse handle(MorphousTask task) {
		logger.debug("Handling Atomic Switch MorphouTask {}", task);
		MorphousTaskResponse response = new MorphousTaskResponse();
		response.status = MorphousTaskResponseStatus.SUCCESSFUL;
		response.taskUuid = task.taskUuid;
		
////		List<Range<Token>> ranges = (List<Range<Token>>) StorageService.instance.getPrimaryRangesForEndpoint(task.keyspace, FBUtilities.getBroadcastAddress());		
//		Collection<Range<Token>> ranges = Util.getNthRangesForLocalNode(task.keyspace, 1);
//		try {
//			insertLocalRangesOnTemporaryCF(task.keyspace, task.columnFamily, Morphous.tempColumnFamilyName(task.columnFamily), task.newPartitionKey	, ranges);
//		} catch (Exception e) {
//			response.message = Throwables.getStackTraceAsString(e);
//			response.status = MorphousTaskResponseStatus.FAILED;
//		}
		return response;
	}

}
