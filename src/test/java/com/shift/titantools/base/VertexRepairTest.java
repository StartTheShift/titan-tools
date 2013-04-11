package com.shift.titantools.base;

import com.shift.titantools.TitanGraphTools;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.query.SimpleTitanQuery;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import org.apache.commons.configuration.Configuration;

import org.junit.Test;
import junit.framework.Assert;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
        Object state = TitanGraphTools.getSystemPropertyValue(v1, SystemKey.VertexState);
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

    /**
     * Tests that corrupt vertex relations are detected and removed
     *
     * @throws Exception
     */
    @Test
    public void testTestCorruptPropertyRepair() throws Exception {

        TitanGraphTools fx;
        fx = new TitanGraphTools((StandardTitanGraph)graphdb);

        //create the properties
        TitanKey name = fx.makeType("name", String.class, true, false);
        TitanKey other = fx.makeType("other", String.class, false, false);

        //create the vertices
        TitanVertex v1 = tx.addVertex();
        v1.addProperty("name", "blake");
        v1.addProperty("other", "toCorrupt");

        TitanVertex v2 = tx.addVertex();
        v2.addProperty("name", "eric");

        TitanVertex v3 = tx.addVertex();
        v3.addProperty("name", "jon");
        tx.commit();

        //force deletion of the 'other' type
        tx = graphdb.newTransaction();
        fx = new TitanGraphTools((StandardTitanGraph)graphdb);
        InternalTitanTransaction itx = (InternalTitanTransaction) graphdb.newTransaction();
        StoreTransaction stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();
        other = (TitanKey) graphdb.getVertex(other.getID());
        SimpleTitanQuery sq = new SimpleTitanQuery((InternalTitanVertex) other);
        List<Entry> entries = fx.queryForEntries(sq.clone(), stx);
        ByteBuffer key = IDHandler.getKey(other.getID());
        List<ByteBuffer> deletions = new LinkedList<ByteBuffer>();
        for (Entry entry: entries) deletions.add(entry.getColumn());
        fx.getBackend().getEdgeStore().mutate(key, null, deletions, stx);
        stx.commit();
        tx.commit();
        clopen();

        //check that it's gone
        tx = graphdb.newTransaction();
        other = (TitanKey) tx.getVertex(other.getID());
        Assert.assertNull(other);

        //check that the vertex with the deleted property is bad
        v1 = tx.getVertex(v1.getID());
        try {
            v1.getProperties();
            Assert.fail();
        } catch (NullPointerException ex) {
            //exception expected
        }
        tx.commit();

        //repair
        tx = graphdb.newTransaction();
        fx = new TitanGraphTools((StandardTitanGraph)graphdb);
        fx.cleanVertices();
        tx.commit();

        tx = graphdb.newTransaction();

        //check that the vertex with the deleted property is bad
        v1 = tx.getVertex(v1.getID());
        try {
            v1.getProperties();
        } catch (NullPointerException ex) {
            Assert.fail();
        }

        //this should fail because the property name is still
        //in the index
        try {
            Assert.assertNull(v1.getProperty("other"));
            Assert.fail();
        } catch (Exception e) {
            //exception expected
        }
        tx.commit();

        //clean up the type name index
        tx = graphdb.newTransaction();
        fx = new TitanGraphTools((StandardTitanGraph)graphdb);
        fx.repairTypeIndex();
        tx.commit();

        //re-check, this should work now
        tx = graphdb.newTransaction();
        v1 = tx.getVertex(v1.getID());
        Assert.assertNull(v1.getProperty("other"));
        Assert.assertEquals("blake", v1.getProperty("name"));
        tx.commit();
    }
}
