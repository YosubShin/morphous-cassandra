package edu.uiuc.dprg.morphous;

import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.WrappedRunnable;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uiuc.dprg.morphous.MessageSender.MorphousTask;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
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
    public FutureTask<Object> createMorphousTask(final String keyspace, final String columnFamily, final MorphousConfiguration config) {
        logger.debug("Creating a morphous task with keyspace={}, columnFamily={}, configuration={}", keyspace, columnFamily, config);
        return new FutureTask<Object>(new WrappedRunnable() {
            @Override
            protected void runMayThrow() throws Exception {
                String message = String.format("Starting morphous command for keyspace %s, column family %s, configuration %s", keyspace, columnFamily, config.toString());
                logger.info(message);
                try {
                	MorphousTask morphousTask = new MorphousTask();
                	MessageSender.instance().sendMorphousTaskToAllEndpoints(morphousTask);
                } catch(Exception e) {
                    logger.error("Execption occurred {}", e);
                    throw new RuntimeException(e);
                }

            }
        }, null);
    }


    @SuppressWarnings("rawtypes")
	private InetAddress getNewDestinationNodeForValue(ByteBuffer value) throws CharacterCodingException {
        logger.debug("Looking for new destination value for value : {}", ByteBufferUtil.string(value));
        // Mimics SimpleStrategy's implementation
        Token token = StorageService.getPartitioner().getToken(value);
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        ArrayList<Token> tokens = metadata.sortedTokens();
        Iterator<Token> iter = TokenMetadata.ringIterator(tokens, token, false);

        // Pick the node after the primary node.(Secondary node)
        Token primaryToken = iter.next();

        InetAddress endpoint = metadata.getEndpoint(primaryToken);
        logger.debug("Token : {}, sorted tokens : {}, primary Token : {}, endpoint : {}", token, tokens, primaryToken, endpoint);
        return endpoint;
    }

    public static ByteBuffer getColumnNameByteBuffer(ColumnFamily data, String columnName) {
        ByteBuffer columnNameByteBuffer = null;
        for (ByteBuffer byteBuffer : data.getColumnNames()) {
            try {
//                logger.debug("column name : {}", ByteBufferUtil.string(byteBuffer, Charset.defaultCharset()));
                if (ByteBufferUtil.string(byteBuffer).contains(columnName)) {
                    columnNameByteBuffer = byteBuffer;
                }
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }
        return columnNameByteBuffer;
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
