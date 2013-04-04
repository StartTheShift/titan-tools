package com.shift.titantools.base;

import com.thinkaurelius.titan.graphdb.TitanGraphTestCommon;
import org.apache.commons.configuration.Configuration;

/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 4/2/13
 * Time: 3:19 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class GraphTest extends TitanGraphTestCommon {

    public GraphTest(Configuration configuration) {
        super(configuration);
    }
}
