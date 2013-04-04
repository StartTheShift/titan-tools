package com.shift.titantools.cassandra;

import com.shift.titantools.base.IndexRepairTest;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;
import static com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager.*;

public class CassandraIndexRepairTest extends IndexRepairTest {

    public static Configuration getConfig() {
        Configuration config = new BaseConfiguration();
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "embeddedcassandra");
        config.subset(STORAGE_NAMESPACE).addProperty(
            CASSANDRA_CONFIG_DIR_KEY,
            "file://" + System.getProperty("user.dir") + "/src/test/resources/config/cassandra.yaml"
        );
        return config;
    }

    public CassandraIndexRepairTest() {
        super(getConfig());
    }
}
