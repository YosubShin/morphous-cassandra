package edu.uiuc.dprg.morphous;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ColumnDefinition.Type;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTask;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskCallback;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponse;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskResponseStatus;
import edu.uiuc.dprg.morphous.MorphousTaskMessageSender.MorphousTaskType;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.CharacterCodingException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Daniel on 6/9/14.
 */
public class Morphous {
    private static Morphous instance = new Morphous();
    private static final Logger logger = LoggerFactory.getLogger(Morphous.class);

    public MorphousConfiguration configuration;

    private Morphous() {
    }

    public static Morphous instance() {
        return instance;
    }


    /**
     * Caveat : When we change the partition key to something else, we also need to recreate index, since index is a table that stores map from the column being indexed to the old primary key
     * @param keyspace
     * @param columnFamily
     * @param config
     * @return
     */
    public FutureTask<Object> createAsyncInsertMorphousTask(final String keyspace, final String columnFamily, final MorphousConfiguration config) {
        logger.debug("Creating a morphous task with keyspace={}, columnFamily={}, configuration={}", keyspace, columnFamily, config);
        return new FutureTask<Object>(new WrappedRunnable() {
            @Override
            protected void runMayThrow() throws Exception {
                String message = String.format("Starting morphous command for keyspace %s, column family %s, configuration %s", keyspace, columnFamily, config.toString());
                logger.info(message);
                try {
                	createTempColumnFamily(keyspace, columnFamily, config.columnName);
                	
                	MorphousTask morphousTask = new MorphousTask();
                	morphousTask.taskType = MorphousTaskType.INSERT;
                	morphousTask.keyspace = keyspace;
                	morphousTask.columnFamily = columnFamily;
                	morphousTask.newPartitionKey = config.columnName;
                	morphousTask.callback = getInsertMorphousTaskCallback();
                	MorphousTaskMessageSender.instance().sendMorphousTaskToAllEndpoints(morphousTask);
                } catch(Exception e) {
                    logger.error("Execption occurred {}", e);
                    throw new RuntimeException(e);
                }

            }
        }, null);
    }
    
    public MorphousTaskCallback getInsertMorphousTaskCallback() {
    	return new MorphousTaskCallback() {
			
			@Override
			public void callback(MorphousTask task,
					Map<InetAddress, MorphousTaskResponse> responses) {
				logger.debug("The InsertMorphousTask {} is done! Now doing the next step...", task);
				MorphousTask morphousTask = new MorphousTask();
            	morphousTask.taskType = MorphousTaskType.ATOMIC_SWITCH;
            	morphousTask.keyspace = task.keyspace;
            	morphousTask.columnFamily = task.columnFamily;
            	morphousTask.newPartitionKey = task.newPartitionKey;
            	//TODO TBD
            	morphousTask.callback = null;
            	MorphousTaskMessageSender.instance().sendMorphousTaskToAllEndpoints(morphousTask);
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

    public MorphousConfiguration parseMorphousConfiguration(String configString) {
        logger.debug("Parsing Morphous config {}", configString);
        MorphousConfiguration config = new MorphousConfiguration();
        if (configString == null || configString.isEmpty()) {
            return config;
        }
        try {
            JSONObject json = (JSONObject) new JSONParser().parse(configString);
            String columnName = (String) json.get("column");

            config.columnName = columnName;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return config;
    }

    public void migrateColumnFamilyDefinitionToUseNewPartitonKey(String keyspaceName, String columnFamilyName, String newPartitionKeyName) {
	        Keyspace keyspace = Keyspace.open(keyspaceName);
	        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamilyName);
	        CFMetaData meta = cfs.metadata.clone();
	
	        logger.debug("Migrating CFMetaData : {} with new partition key {}", meta, newPartitionKeyName);
	        changePartitionKeyOfCFMetaData(meta, newPartitionKeyName);
	
	        CFMetaData oldMeta = cfs.metadata;
	        RowMutation rm = edu.uiuc.dprg.morphous.Util.invokePrivateMethodWithReflection(MigrationManager.instance, "addSerializedKeyspace", oldMeta.toSchemaUpdate(meta, FBUtilities.timestampMicros(), false), keyspaceName);
	        edu.uiuc.dprg.morphous.Util.invokePrivateMethodWithReflection(MigrationManager.instance, "announce", rm);
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
	            String deserializedColumnName = ByteBufferUtil.string(columnDefinition.name);
	            logger.debug("ColumnDefinition for column {} : {}", deserializedColumnName, columnDefinition);
	            if (columnDefinition.type == ColumnDefinition.Type.PARTITION_KEY) {
	            	// Change old partiton key to regular column
	                newRegularColumn = new ColumnDefinition(columnDefinition.name, columnDefinition.getValidator(), 0, ColumnDefinition.Type.REGULAR);
	            } else if (deserializedColumnName.equals(newPartitionKey)) {
	            	// Change old regular column that matches newPartitionKeyName to new PartitonKey 
	                newPartitionKeyColumn = new ColumnDefinition(columnDefinition.name, columnDefinition.getValidator(), null, ColumnDefinition.Type.PARTITION_KEY);
	            }
	        } catch (CharacterCodingException e) {
	            throw new RuntimeException(e);
	        }
	    }
	    assert newRegularColumn != null;
	    assert newPartitionKeyColumn != null;
	    meta.addOrReplaceColumnDefinition(newPartitionKeyColumn);
	    meta.addOrReplaceColumnDefinition(newRegularColumn);
	}

	public void moveSSTablesFromDifferentCFAndRemovePreviousSSTables(Keyspace keyspace, ColumnFamilyStore from,
			ColumnFamilyStore to) {
		Directories originalDirectories = Directories.create(keyspace.getName(), from.name);
		Directories destDirectory = Directories.create(keyspace.getName(), to.name);
		
		Collection<SSTableReader> destNewSSTables = new HashSet<>();
		
		for (Entry<Descriptor, Set<Component>> entry : originalDirectories.sstableLister().list().entrySet()) {
			Descriptor srcDescriptor = entry.getKey();
			Descriptor destDescriptor = new Descriptor(
					destDirectory.getDirectoryForNewSSTables(),
					keyspace.getName(),
					to.name,
					((AtomicInteger) edu.uiuc.dprg.morphous.Util.getPrivateFieldWithReflection(to, "fileIndexGenerator")).incrementAndGet(), 
					false);
			logger.debug("Moving SSTable {} to {}", srcDescriptor.directory, destDescriptor.directory);
			for (Component component : entry.getValue()) {
				FileUtils.renameWithConfirm(srcDescriptor.filenameFor(component), destDescriptor.filenameFor(component));
			}
			
			try {
				destNewSSTables.add(SSTableReader.open(destDescriptor));
			} catch (IOException e) {
				logger.error("Exception while creating a new SSTableReader {}", e);
				throw new RuntimeException(e);
			}
		}
		
		// Remove SSTable from memory in temporary CF
		for (File directory : originalDirectories.getCFDirectories()) {
			edu.uiuc.dprg.morphous.Util.invokePrivateMethodWithReflection(from.getDataTracker(), "removeUnreadableSSTables", directory);
		}
		
		// Add copied SSTable to destination CF, and remove old SSTables from destination CF
		Set<SSTableReader> destOldSSTables = to.getDataTracker().getSSTables();
		to.getDataTracker().replaceCompactedSSTables(destOldSSTables, destNewSSTables, OperationType.UNKNOWN);
		
	}

	public static class MorphousConfiguration {
        public String columnName;

        @Override
        public String toString() {
            return "MorphousConfiguration{" +
                    "columnName='" + columnName + '\'' +
                    '}';
        }
    }
}
