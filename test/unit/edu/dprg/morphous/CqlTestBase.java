package edu.dprg.morphous;

import java.io.IOException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;

public class CqlTestBase  extends SchemaLoader {
    protected static EmbeddedCassandraService cassandra;

    protected static void startCassandra() throws IOException
    {
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        cassandra = new EmbeddedCassandraService();
        cassandra.start();
    }
}
