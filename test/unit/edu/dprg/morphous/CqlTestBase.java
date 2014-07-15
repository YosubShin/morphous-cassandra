package edu.dprg.morphous;

import java.io.IOException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CqlTestBase  extends SchemaLoader {
    protected static EmbeddedCassandraService cassandra;

    protected static Cassandra.Client getClient() throws TTransportException
    {
        TTransport tr = new TFramedTransport(new TSocket("localhost", DatabaseDescriptor.getRpcPort()));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        return client;
    }

    protected static void startCassandra() throws IOException
    {
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        cassandra = new EmbeddedCassandraService();
        cassandra.start();
    }
    
    protected static CqlResult executeCql3Statement(String statement) {
    	try {
    		Cassandra.Client client = getClient();
			return client.execute_cql3_query(ByteBufferUtil.bytes(statement), Compression.NONE, ConsistencyLevel.ONE);
		} catch (TException e) {
			throw new RuntimeException(e);
		}
    }
}
