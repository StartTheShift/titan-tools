package com.shift.titantools.base;

import com.google.common.collect.Lists;
import com.shift.titantools.TitanGraphTools;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import junit.framework.Assert;
import org.apache.commons.configuration.Configuration;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class IndexRepairTest extends GraphTest {

    public IndexRepairTest(Configuration configuration) { super(configuration); }

    private TitanKey makeType(String name, Class<?> type, Boolean indexed, Boolean unique) {
        TypeMaker t = tx.makeType().name(name).simple().functional();
        if (unique) t = t.unique();
        if (indexed) t = t.indexed();
        return t.dataType(type).group(TypeGroup.DEFAULT_GROUP).makePropertyKey();
    }

    @Test
    public void testUniqueIndexRepair() throws Exception {
        TitanKey id = makeType("vid", String.class, true, true);

        int numVertices = 10;
        TitanVertex[] vertices = new TitanVertex[numVertices];
        TitanProperty[] properties = new TitanProperty[numVertices];
        for (int i=0; i<numVertices; i++) {
            TitanVertex v = tx.addVertex();
            TitanProperty p = v.addProperty(id, "id-" + i);
            vertices[i] = v;
            properties[i] = p;
        }

        TitanGraphTools g = new TitanGraphTools((StandardTitanGraph) graphdb);

        KeyColumnValueStore indexStore = g.getBackend().getVertexIndexStore();

        InternalTitanTransaction itx = (InternalTitanTransaction) graphdb.newTransaction();
        StoreTransaction stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();
        //add a fake unique index entry
        String fakeId = "id-20";
        long fakeVertexId = 5678;
        indexStore.mutate(
                //the attribute is the key
                g.getIndexKey(fakeId),

                //key additions
                //the column->value is the id property mapped to the vertex id
                Lists.newArrayList(
                        new Entry(g.getKeyedIndexColumn(id), VariableLong.positiveByteBuffer(fakeVertexId))
                ),

                //no deletions
                null,

                //storage transaction handle
                stx
        );

        //this is usually handled internally
        itx.commit();

        //this commits the transaction automatically
        clopen();

        itx = (InternalTitanTransaction) graphdb.newTransaction();
        long[] matches = ((StandardTitanGraph) graphdb).indexRetrieval(fakeId, id, itx);

        Assert.assertEquals(matches.length, 1);
        Assert.assertEquals(matches[0], fakeVertexId);

        //look through the indexed keys
        g = new TitanGraphTools((StandardTitanGraph) graphdb);
        g.repairType(id);

        clopen();
        itx = (InternalTitanTransaction) graphdb.newTransaction();
        matches = ((StandardTitanGraph) graphdb).indexRetrieval(fakeId, id, itx);

        Assert.assertEquals(matches.length, 0);
    }

    /**
     * Tests that an index associating an incorrect value with a vertex is repaired,
     * the erroneous value needs to be removed and the correct one inserted.
     *
     * This looks for cases where, for the property 'name', the value on the vertex
     * is 'blake', but looking up 'eric' returns the vertex
     *
     * @throws Exception
     */
    @Test
    public void testIncorrectIndexedValueRepair() throws Exception {


    }
}
