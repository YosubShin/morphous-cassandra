package edu.uiuc.dprg.morphous;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ColumnDefinition.Type;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.exceptions.ConfigurationException;
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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.concurrent.FutureTask;

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
				MorphousTask newMorphousTask = new MorphousTask();
            	newMorphousTask.taskType = MorphousTaskType.ATOMIC_SWITCH;
            	newMorphousTask.keyspace = task.keyspace;
            	newMorphousTask.columnFamily = task.columnFamily;
            	newMorphousTask.newPartitionKey = task.newPartitionKey;
            	//TODO TBD for catching up
            	newMorphousTask.callback = null;
            	
            	Keyspace keyspace = Keyspace.open(task.keyspace);
        		ColumnFamilyStore originalCfs = keyspace.getColumnFamilyStore(task.columnFamily);
        		String originalPartitionKey = getOriginalPartitionKeyName(originalCfs);
            	migrateColumnFamilyDefinitionToUseNewPartitonKey(task.keyspace, originalCfs.name, task.newPartitionKey);
            	migrateColumnFamilyDefinitionToUseNewPartitonKey(task.keyspace, tempColumnFamilyName(originalCfs.name), originalPartitionKey);
            	
            	MorphousTaskMessageSender.instance().sendMorphousTaskToAllEndpoints(newMorphousTask);
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
    
    public static String getOriginalPartitionKeyName(ColumnFamilyStore cfs) {
    	String originalPartitionKey = null;
		try {
			originalPartitionKey = ByteBufferUtil.string(((ByteBuffer) cfs.metadata.partitionKeyColumns().get(0).name.rewind()).asReadOnlyBuffer());
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

            config.columnName = columnName;
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
	        	//TODO Weird why do I need to rewind() here? Apparently without this the partition key 'id' becomes pointing to back.
	        	columnDefinition.name.rewind();
	            String deserializedColumnName = ByteBufferUtil.string(columnDefinition.name.asReadOnlyBuffer());
	            logger.debug("ColumnDefinition for column {} : {}", deserializedColumnName, columnDefinition);
	            if (columnDefinition.type == ColumnDefinition.Type.PARTITION_KEY) {
	            	// Change old partiton key to regular column
	            	newRegularColumn = new ColumnDefinition(columnDefinition.name.duplicate(), columnDefinition.getValidator(), 0, ColumnDefinition.Type.REGULAR);
	            } else if (deserializedColumnName.equals(newPartitionKey)) {
	            	// Change old regular column that matches newPartitionKeyName to new PartitonKey 
	                newPartitionKeyColumn = new ColumnDefinition(columnDefinition.name.duplicate(), columnDefinition.getValidator(), null, ColumnDefinition.Type.PARTITION_KEY);
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
