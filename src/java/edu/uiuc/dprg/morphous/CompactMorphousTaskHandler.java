package edu.uiuc.dprg.morphous;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTask;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponse;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponseStatus;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CompactMorphousTaskHandler implements MorphousTaskHandler {
	private static final Logger logger = LoggerFactory.getLogger(CompactMorphousTaskHandler.class);

	@SuppressWarnings("rawtypes")
	@Override
	public MorphousTaskResponse handle(MorphousTask task) {
		logger.debug("Handling CompactMorphousTask {}", task);
		MorphousTaskResponse response = new MorphousTaskResponse();
		response.status = MorphousTaskResponseStatus.SUCCESSFUL;
		response.taskUuid = task.taskUuid;

		ColumnFamilyStore originalCfs = Keyspace.open(task.keyspace).getColumnFamilyStore(task.columnFamily);
        ColumnFamilyStore tempCfs = Keyspace.open(task.keyspace).getColumnFamilyStore(Morphous.tempColumnFamilyName(task.columnFamily));

        originalCfs.disableAutoCompaction();
        tempCfs.disableAutoCompaction();

        try {
            originalCfs.forceBlockingFlush();
            originalCfs.forceMajorCompaction();
        } catch (Exception e) {
            logger.error("Error while compacting the node with exception {}", e);
            response.message = Throwables.getStackTraceAsString(e);
            response.status = MorphousTaskResponseStatus.FAILED;
        }

		return response;
	}
}
