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
        itx = (InternalTitanTransaction) graphdb.newTransaction();
        stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();
        indexStore = g.getBackend().getVertexIndexStore();
        RecordIterator<ByteBuffer> keys = indexStore.getKeys(stx);
        while (keys.hasNext()) {
            ByteBuffer key = keys.next();
            List<ByteBuffer> deletions = new ArrayList<ByteBuffer>();
            ByteBuffer startCol = VariableLong.positiveByteBuffer(id.getID());
            List<Entry> columns = indexStore.getSlice(
                    key,
                    startCol,
                    ByteBufferUtil.nextBiggerBuffer(startCol),
                    stx
            );
            for (Entry entry : columns) {
                int x = 0;
                long eid = VariableLong.readPositive(entry.getValue());
                TitanVertex v = (TitanVertex) graphdb.getVertex(eid);
                if (v == null) {
                    deletions.add(entry.getColumn());
                    System.out.println("deleted vertex found in index");
                } else {
                    //verify that the given property matches
                    Object value = v.getProperty(id);
                    ByteBuffer indexKey = g.getIndexKey(value);
                    if (!Arrays.equals(key.array(), indexKey.array())) {
                        deletions.add(entry.getColumn());
                        System.out.println("value mismatch found in index");
                    }
                }
            }

            if (deletions.size() > 0) {
                indexStore.mutate(key, null, deletions, stx);
            }
        }

        //cleanup
        itx.commit();

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
