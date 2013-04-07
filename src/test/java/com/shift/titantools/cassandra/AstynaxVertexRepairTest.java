package com.shift.titantools.cassandra;

import com.shift.titantools.base.VertexRepairTest;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import static com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager.*;

public class AstynaxVertexRepairTest extends VertexRepairTest {
    public static Configuration getConfig() {
        Configuration config = new BaseConfiguration();
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "cassandra");
        config.subset(STORAGE_NAMESPACE).addProperty(HOSTNAME_KEY, "127.0.0.1");
        config.subset(STORAGE_NAMESPACE).addProperty(KEYSPACE_KEY, "titantoolstest");
        config.addProperty(AUTO_TYPE_KEY, "none");
        return config;
    }

    public AstynaxVertexRepairTest() {
        super(getConfig());
    }
}
