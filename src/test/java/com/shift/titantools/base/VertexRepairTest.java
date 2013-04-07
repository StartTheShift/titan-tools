package com.shift.titantools.base;

import com.shift.titantools.TitanGraphTools;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import org.apache.commons.configuration.Configuration;

import org.junit.Test;
import junit.framework.Assert;

public abstract class VertexRepairTest extends GraphTest {

    protected VertexRepairTest(Configuration configuration) { super(configuration); }

    /**
     * Test that inconsistent vertices are first created, then deleted
     *
     * @throws Exception
     */
    @Test
    public void testVertexDeletion() throws Exception {

        TitanGraphTools fx;
        fx = new TitanGraphTools((StandardTitanGraph)graphdb);

        fx.makeType("name", String.class, true, false);

        //add the doomed vertex
        tx = graphdb.newTransaction();
        TitanVertex v = tx.addVertex();
        TitanProperty p = v.addProperty("name", "oldName");

        //this vertex should not be touched
        TitanVertex vv = tx.addVertex();
        vv.addProperty("name", "not any trouble");

        tx.commit();

        //load it in the first transaction
        TitanTransaction tx1 = graphdb.newTransaction();
        TitanVertex v1 = tx1.getVertex(v.getID());

        //check the vertex state
        Object state = TitanGraphTools.getSystemProperty(v1, SystemKey.VertexState);
        Assert.assertTrue(state != null);

        //load it in a second transaction
        TitanTransaction tx2 = graphdb.newTransaction();
        TitanVertex v2 = tx2.getVertex(v.getID());

        //delete it in the first transaction
        tx1.removeVertex(v1);
        tx1.commit();

        //add properties in the second transaction
        v2.setProperty("name", "newName");
        tx2.commit();

        //still there :/
        TitanTransaction tx3 = graphdb.newTransaction();
        TitanVertex v3 = tx3.getVertex(v.getID());
        Assert.assertFalse(v3 == null);

        //clean it up
        fx = new TitanGraphTools((StandardTitanGraph)graphdb);
        fx.cleanVertices();

        //it should be gone
        TitanTransaction tx4 = graphdb.newTransaction();
        TitanVertex v4 = tx4.getVertex(v.getID());
        Assert.assertEquals(null, v4);

        //check that the integrity check vertex still exists
        vv = tx4.getVertex(vv.getID());
        Assert.assertFalse(vv == null);
    }
}
