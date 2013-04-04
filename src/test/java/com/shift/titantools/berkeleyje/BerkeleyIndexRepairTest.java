package com.shift.titantools.berkeleyje;

import com.shift.titantools.base.IndexRepairTest;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class BerkeleyIndexRepairTest extends IndexRepairTest {

    public static Configuration getConfig() {
        Configuration config = new BaseConfiguration();
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_BACKEND_KEY, "berkeleyje");
        config.subset(STORAGE_NAMESPACE).addProperty(STORAGE_DIRECTORY_KEY, "/tmp/titantoolstest");
        return config;
    }

    public BerkeleyIndexRepairTest() {
        super(getConfig());
    }
}
