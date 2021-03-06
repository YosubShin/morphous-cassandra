package edu.uiuc.dprg.morphous;

import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTask;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskCallback;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponse;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskType;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by Daniel on 6/9/14.
 */
public class Morphous {
    public static final int numConcurrentMorphusMutationHandlerThreads = 32;
    private static Morphous instance = new Morphous();
    private static final Logger logger = LoggerFactory.getLogger(Morphous.class);

    private int numConcurrentRowMutationSenderThreads = 8;
    private ExecutorService executor;

    public MorphousConfiguration configuration;

    private Morphous() {
    }

    public static Morphous instance() {
        return instance;
    }

    private long startTimestamp;

    /**
     * @param keyspace
     * @param columnFamily
     * @param config
     * @return
     */
    public FutureTask<Object> createAsyncMorphousTask(final String keyspace, final String columnFamily, final MorphousConfiguration config) {
        startTimestamp = System.currentTimeMillis(); // Record start time
        logger.info("MorphusTimestamp: MorphusStartAt {}", startTimestamp);
        logger.debug("Creating a morphous task with keyspace={}, columnFamily={}, configuration={}", keyspace, columnFamily, config);
        updateNumConcurrentRowMutationSenderThreads(config.numMorphusMutationSenderThreads);

        return new FutureTask<Object>(new WrappedRunnable() {
            @Override
            protected void runMayThrow() throws Exception {
                logger.info("Starting morphous command for keyspace {}, column family {}, configuration {}", keyspace, columnFamily, config);
                try {
                    createTempColumnFamily(keyspace, columnFamily, config.columnName);
                    // Wait until the create table command propagates
                    Thread.sleep(8000);

                    MorphousTask morphousTask = new MorphousTask();
                    if (config.shouldCompact) {
                        morphousTask.taskType = MorphousTaskType.COMPACT;
                        morphousTask.callback = getCompactMorphousTaskCallback();
                    } else {
                        morphousTask.taskType = MorphousTaskType.INSERT;
                        morphousTask.callback = getInsertMorphousTaskCallback();
                    }
                    morphousTask.keyspace = keyspace;
                    morphousTask.columnFamily = columnFamily;
                    morphousTask.newPartitionKey = config.columnName;
                    morphousTask.taskStartedAtInMicro = System.currentTimeMillis() * 1000;
                    morphousTask.numConcurrentRowMutationSenderThreads = Morphous.instance().numConcurrentRowMutationSenderThreads;
                    MorphousTaskMessageSender.instance().sendMorphousTaskToAllEndpoints(morphousTask);
                } catch(Exception e) {
                    logger.error("Execption occurred {}", e);
                    throw new RuntimeException(e);
                }

            }
        }, null);
    }


    public MorphousTaskCallback getCompactMorphousTaskCallback() {
        return new MorphousTaskCallback() {
            @Override
            public void callback(MorphousTask task, Map<InetAddress, MorphousTaskResponse> responses) {
                logger.info("MorphusTimestamp: CompactMorphusTask {}", System.currentTimeMillis());
                logger.info("CompactMorphousTask is over in {}ms (since reconfiguration was started)", System.currentTimeMillis() - startTimestamp);

                logger.info("CompactMorphousTask is over. Start InsertMorphousTask for keyspace {}, column family {}", task.keyspace, task.columnFamily);
                try {
                    MorphousTask morphousTask = new MorphousTask();
                    morphousTask.taskType = MorphousTaskType.INSERT;
                    morphousTask.keyspace = task.keyspace;
                    morphousTask.columnFamily = task.columnFamily;
                    morphousTask.newPartitionKey = task.newPartitionKey;
                    morphousTask.callback = getInsertMorphousTaskCallback();
                    morphousTask.taskStartedAtInMicro = task.taskStartedAtInMicro;
                    morphousTask.numConcurrentRowMutationSenderThreads = Morphous.instance().numConcurrentRowMutationSenderThreads;
                    MorphousTaskMessageSender.instance().sendMorphousTaskToAllEndpoints(morphousTask);
                } catch(Exception e) {
                    logger.error("Execption occurred {}", e);
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public void setWriteLockOnColumnFamily(String ksName, String columnFamily, boolean locked) {
        logger.info("Locking/Unlocking Keysapce {}, ColumnFamily {}. isLocked = {}", ksName, columnFamily, locked);
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(ksName));
        ColumnFamily cf = rm.addOrGet(CFMetaData.MorphousStatusCf);
        long timestamp = FBUtilities.timestampMicros() + 1;

        cf.addColumn(Column.create("", timestamp, columnFamily, "")); // Since column family name is part of composite key
        cf.addColumn(Column.create(locked, timestamp, columnFamily, "swapping"));
        // No need to include keyspace because it's partition key itself

        Util.invokePrivateMethodWithReflection(MigrationManager.instance, "announce", rm);
    }
    
    /**
     * Get callback for the InsertMorphousTask. It should generate the next stage MorphousTask.
     * @return
     */
    public MorphousTaskCallback getInsertMorphousTaskCallback() {
    	return new MorphousTaskCallback() {
			
			@Override
			public void callback(MorphousTask task, Map<InetAddress, MorphousTaskResponse> responses) {
                logger.info("MorphusTimestamp: InsertMorphusTask {}", System.currentTimeMillis());
                logger.info("InsertMorphousTask is over in {}ms (since reconfiguration was started)", System.currentTimeMillis() - startTimestamp);

				logger.debug("The InsertMorphousTask {} is done! Now doing the next step", task);
				
				// Create AtomicSwitchMorphousTask
				MorphousTask newMorphousTask = new MorphousTask();
            	newMorphousTask.taskType = MorphousTaskType.ATOMIC_SWITCH;
            	newMorphousTask.keyspace = task.keyspace;
            	newMorphousTask.columnFamily = task.columnFamily;
            	newMorphousTask.newPartitionKey = task.newPartitionKey;
            	newMorphousTask.callback = getAtomicSwitchMorphousTaskCallback();
            	newMorphousTask.taskStartedAtInMicro = task.taskStartedAtInMicro;
                newMorphousTask.numConcurrentRowMutationSenderThreads = Morphous.instance().numConcurrentRowMutationSenderThreads;
            	
            	Keyspace keyspace = Keyspace.open(task.keyspace);
        		ColumnFamilyStore originalCfs = keyspace.getColumnFamilyStore(task.columnFamily);
        		String originalPartitionKey = getPartitionKeyName(originalCfs);
                // Put lock on write requests on this column family
                setWriteLockOnColumnFamily(task.keyspace, task.columnFamily, true);
            	migrateColumnFamilyDefinitionToUseNewPartitonKey(task.keyspace, originalCfs.name, task.newPartitionKey);
            	migrateColumnFamilyDefinitionToUseNewPartitonKey(task.keyspace, tempColumnFamilyName(originalCfs.name), originalPartitionKey);

            	MorphousTaskMessageSender.instance().sendMorphousTaskToAllEndpoints(newMorphousTask);
			}
		};
    }

    public MorphousTaskCallback getAtomicSwitchMorphousTaskCallback() {
        return new MorphousTaskCallback() {

            @Override
            public void callback(MorphousTask task, Map<InetAddress, MorphousTaskResponse> responses) {
                // Unlock write lock on this column family
                setWriteLockOnColumnFamily(task.keyspace, task.columnFamily, false);
                logger.info("MorphusTimestamp: AtomicSwitchMorphusTask {}", System.currentTimeMillis());
                logger.info("AtomicSwitchMorphousTask is over in {}ms (since reconfiguration was started)", System.currentTimeMillis() - startTimestamp);

                logger.debug("The AtomicSwitchMorphousTask {} is done! Now doing the next step", task);
                // Create CatchupMorphousTask
                MorphousTask newMorphousTask = new MorphousTask();
                newMorphousTask.taskType = MorphousTaskType.CATCH_UP;
                newMorphousTask.keyspace = task.keyspace;
                newMorphousTask.columnFamily = task.columnFamily;
                newMorphousTask.newPartitionKey = task.newPartitionKey;
                newMorphousTask.callback = getCatchupMorphousTaskCallback();
                newMorphousTask.taskStartedAtInMicro = task.taskStartedAtInMicro;
                newMorphousTask.numConcurrentRowMutationSenderThreads = Morphous.instance().numConcurrentRowMutationSenderThreads;

                MorphousTaskMessageSender.instance().sendMorphousTaskToAllEndpoints(newMorphousTask);
            }
        };
    }

    public MorphousTaskCallback getCatchupMorphousTaskCallback() {
        return new MorphousTaskCallback() {

            @Override
            public void callback(MorphousTask task, Map<InetAddress, MorphousTaskResponse> responses) {
                logger.debug("The CatchupMorphousTask {} is done.", task);
                for (Map.Entry<InetAddress, MorphousTaskResponse> entry : responses.entrySet()) {
                    logger.debug("From: {}, Response : {}", entry.getKey(), entry.getValue());
                }
                logger.info("MorphusTimestamp: CatchupMorphusTask {}", System.currentTimeMillis());
                logger.info("CatchupMorphousTask is over in {}ms (since reconfiguration was started)", System.currentTimeMillis() - startTimestamp);
            }
        };
    }
    
    public void createTempColumnFamily(String ksName, String oldCfName, String newPartitionKey) {
    	logger.debug("Creating a temporary column family for changing Column Family {}'s partition key to {}", oldCfName, newPartitionKey);
    	CFMetaData oldCfm = Keyspace.open(ksName).getColumnFamilyStore(oldCfName).metadata;

    	String tempCfName = tempColumnFamilyName(oldCfName);
    	CFMetaData cfm = createNewCFMetaDataFromOldCFMetaDataWithNewCFNameAndNewPartitonKey(oldCfm, tempCfName, newPartitionKey);
    	createNewColumnFamilyWithCFMetaData(cfm);
    }
    
    public static String tempColumnFamilyName(String originalCfName) {
    	return "temp_" + originalCfName;
    }
    
    public static ByteBuffer getPartitionKeyNameByteBuffer(ColumnFamilyStore cfs) {
		return getPartitionKeyNameByteBuffer(cfs.metadata);
    }
    
    public static ByteBuffer getPartitionKeyNameByteBuffer(CFMetaData metadata) {
		return ((ByteBuffer) metadata.partitionKeyColumns().get(0).name.asReadOnlyBuffer());
    }
    
    public static String getPartitionKeyName(ColumnFamilyStore cfs) {
    	String originalPartitionKey = null;
		try {
			originalPartitionKey = ByteBufferUtil.string(getPartitionKeyNameByteBuffer(cfs));
			return originalPartitionKey;
		} catch (CharacterCodingException e) {
			throw new MorphousException("Unable to decode partition key's name", e);
		}
    }

    public MorphousConfiguration parseMorphousConfiguration(String configString) {
        logger.debug("Parsing Morphous config {}", configString);
        MorphousConfiguration config = new MorphousConfiguration();
        if (configString == null || configString.isEmpty()) {
            return config;
        }
        try {
            JSONObject json = (JSONObject) new JSONParser().parse(configString);
            String columnName = (String) json.get("column");
            boolean shouldCompact = Boolean.parseBoolean((String) json.get("compact"));
            int numMorphusMutationSenderThreads = Integer.parseInt((String) json.get("numMorphusMutationSenderThreads"));

            config.columnName = columnName;
            config.shouldCompact = shouldCompact;
            config.numMorphusMutationSenderThreads = numMorphusMutationSenderThreads;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return config;
    }

    public void createNewColumnFamilyWithCFMetaData(CFMetaData meta) {
		try {
			MigrationManager.announceNewColumnFamily(meta);
		} catch (ConfigurationException e) {
			throw new RuntimeException("Failed to create new Column Family", e);
		}
	}

	/** Create a new CFMetaData
	 *  
	 * @param oldMeta
	 * @param newPartitionKey
	 * @return
	 */
	public CFMetaData createNewCFMetaDataFromOldCFMetaDataWithNewCFNameAndNewPartitonKey(CFMetaData oldMeta, String newCfName, String newPartitionKey) {
		CFMetaData newMeta = new CFMetaData(oldMeta.ksName, newCfName, oldMeta.cfType, oldMeta.comparator);
		
		edu.uiuc.dprg.morphous.Util.invokePrivateMethodWithReflection(newMeta, "copyOpts", newMeta, oldMeta);
		changePartitionKeyOfCFMetaData(newMeta, newPartitionKey);
		
		return newMeta;
	}

	public void changePartitionKeyOfCFMetaData(CFMetaData meta, String newPartitionKey) {
		ColumnDefinition newRegularColumn = null;
	    ColumnDefinition newPartitionKeyColumn = null;
	    for (ColumnDefinition columnDefinition : meta.allColumns()) {
	        try {
	            String deserializedColumnName = ByteBufferUtil.string(columnDefinition.name.asReadOnlyBuffer());
	            logger.debug("ColumnDefinition for column {} : {}", deserializedColumnName, columnDefinition);
	            if (columnDefinition.type == ColumnDefinition.Type.PARTITION_KEY) {
	            	// Change old partiton key to regular column
	            	newRegularColumn = new ColumnDefinition(columnDefinition.name.asReadOnlyBuffer(), columnDefinition.getValidator(), 0, ColumnDefinition.Type.REGULAR);
	            } else if (deserializedColumnName.equals(newPartitionKey)) {
	            	// Change old regular column that matches newPartitionKeyName to new PartitonKey 
	                newPartitionKeyColumn = new ColumnDefinition(columnDefinition.name.asReadOnlyBuffer(), columnDefinition.getValidator(), null, ColumnDefinition.Type.PARTITION_KEY);
	            }
	        } catch (CharacterCodingException e) {
	            throw new RuntimeException(e);
	        }
	    }
	    meta.addOrReplaceColumnDefinition(newPartitionKeyColumn);
	    meta.addOrReplaceColumnDefinition(newRegularColumn);
	}
	
	public void migrateColumnFamilyDefinitionToUseNewPartitonKey(
			String keyspaceName, String columnFamilyName,
			String newPartitionKeyName) {
		Keyspace keyspace = Keyspace.open(keyspaceName);
		ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamilyName);
		CFMetaData meta = cfs.metadata.clone();

		logger.debug("Migrating CFMetaData : {} with new partition key {}",
				meta, newPartitionKeyName);
		changePartitionKeyOfCFMetaData(meta, newPartitionKeyName);
		logger.debug("After changing CFMetaData : {}",
				meta);

		CFMetaData oldMeta = cfs.metadata;
		RowMutation rm = edu.uiuc.dprg.morphous.Util
				.invokePrivateMethodWithReflection(
						MigrationManager.instance,
						"addSerializedKeyspace",
						oldMeta.toSchemaUpdate(meta,
								FBUtilities.timestampMicros(), false),
						keyspaceName);
		
		logger.info("About to announce change on partition key with RowMutation = {}", rm);
		edu.uiuc.dprg.morphous.Util.invokePrivateMethodWithReflection(
				MigrationManager.instance, "announce", rm);
	}

	/**
	 * 
	 * @param rm
	 * @param n one-based number that represents what replication order it has
	 */
	public void sendRowMutationToNthReplicaNode(final RowMutation rm, final int n) {
        getExecutor().submit(new WrappedRunnable() {
            @Override
            protected void runMayThrow() throws Exception {
                InetAddress destinationNode = edu.uiuc.dprg.morphous.Util.getNthReplicaNodeForKey(rm.getKeyspaceName(), rm.key(), n);
                if (FailureDetector.instance.isAlive(destinationNode)) {
                    MessageOut<RowMutation> message = rm.createMessage(MessagingService.Verb.MORPHUS_MUTATION);
                    WriteResponseHandler handler = new WriteResponseHandler(Collections.singletonList(destinationNode), Collections.<InetAddress>emptyList(), ConsistencyLevel.ONE, Keyspace.open(rm.getKeyspaceName()), null, WriteType.SIMPLE);
                    MessagingService.instance().sendRR(message, destinationNode, handler, false); //TODO Maybe use more robust way to send message
                    try {
                        handler.get();
                    } catch (WriteTimeoutException e) {
                        logger.warn("Write timeout exception for Morphus RowMutation {}", rm);
                    }
                }
            }
        });
    }

	public static class MorphousConfiguration {
        public String columnName;
        public boolean shouldCompact;
        public int numMorphusMutationSenderThreads;

        @Override
        public String toString() {
            return "MorphousConfiguration{" +
                    "columnName='" + columnName + '\'' +
                    ", shouldCompact=" + shouldCompact +
                    ", numMorphusMutationSenderThreads=" + numMorphusMutationSenderThreads +
                    '}';
        }
    }

    public ExecutorService getExecutor() {
        if (executor == null) {
            BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(numConcurrentRowMutationSenderThreads);
            RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
            executor =  new ThreadPoolExecutor(numConcurrentRowMutationSenderThreads, numConcurrentRowMutationSenderThreads,
                    0L, TimeUnit.MILLISECONDS, blockingQueue, rejectedExecutionHandler);
        }
        return executor;
    }

    public void updateNumConcurrentRowMutationSenderThreads(int numConcurrentRowMutationSenderThreads) {
        this.numConcurrentRowMutationSenderThreads = numConcurrentRowMutationSenderThreads;
    }
}
