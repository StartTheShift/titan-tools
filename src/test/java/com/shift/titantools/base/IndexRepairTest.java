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
import com.tinkerpop.blueprints.Vertex;
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

        TitanGraphTools fx = new TitanGraphTools((StandardTitanGraph) graphdb);

        KeyColumnValueStore indexStore = fx.getBackend().getVertexIndexStore();

        InternalTitanTransaction itx = (InternalTitanTransaction) graphdb.newTransaction();
        StoreTransaction stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();
        //add a fake unique index entry
        String fakeId = "id-20";
        long fakeVertexId = 5678;
        indexStore.mutate(
                //the attribute is the key
                fx.getIndexKey(fakeId),

                //key additions
                //the column->value is the id property mapped to the vertex id
                Lists.newArrayList(
                        new Entry(fx.getKeyedIndexColumn(id), VariableLong.positiveByteBuffer(fakeVertexId))
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

        Assert.assertEquals(1, matches.length);
        Assert.assertEquals(fakeVertexId, matches[0]);

        //look through the indexed keys
        fx = new TitanGraphTools((StandardTitanGraph) graphdb);
        fx.repairType(id);

        clopen();
        itx = (InternalTitanTransaction) graphdb.newTransaction();
        matches = ((StandardTitanGraph) graphdb).indexRetrieval(fakeId, id, itx);

        Assert.assertEquals(0, matches.length);
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
        TitanKey name = makeType("name", String.class, true, false);
        tx.commit();

        int numVertices = 10;
        tx = graphdb.newTransaction();
        TitanVertex[] vertices = new TitanVertex[numVertices];
        TitanProperty[] properties = new TitanProperty[numVertices];
        for (int i=0; i<numVertices; i++) {
            TitanVertex v = tx.addVertex();
            TitanProperty p = v.addProperty("name", "name-" + i);
            vertices[i] = v;
            properties[i] = p;
        }
        clopen();

        InternalTitanTransaction itx;
        StoreTransaction stx;

        String realName = "name-0";
        String fakeName = "name-1000";

        itx = (InternalTitanTransaction) graphdb.newTransaction();
        stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();

        long[] matches;
        matches = ((StandardTitanGraph) graphdb).indexRetrieval(realName, name, itx);
        Assert.assertEquals(1, matches.length);

        clopen();

        //corrupt index
        TitanGraphTools fx = new TitanGraphTools((StandardTitanGraph) graphdb);

        KeyColumnValueStore indexStore = fx.getBackend().getVertexIndexStore();

        itx = (InternalTitanTransaction) graphdb.newTransaction();
        stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();

        ByteBuffer startColumn = VariableLong.positiveByteBuffer(name.getID());
        List<Entry> entries = indexStore.getSlice(
                fx.getIndexKey(realName),
                startColumn,
                ByteBufferUtil.nextBiggerBuffer(startColumn),
                stx
        );

        //make a fake index entry
        indexStore.mutate(fx.getIndexKey(fakeName), entries, null, stx);

        //delete the real index entry
        List<ByteBuffer> toDelete = new ArrayList<ByteBuffer>(entries.size());
        for (Entry entry: entries) toDelete.add(entry.getColumn());
        indexStore.mutate(fx.getIndexKey(realName), null, toDelete, stx);
        itx.commit();

        clopen();

        itx = (InternalTitanTransaction) graphdb.newTransaction();
        stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();


        //check that the index is busted
        matches = ((StandardTitanGraph) graphdb).indexRetrieval(fakeName, name, itx);
        Assert.assertEquals(1, matches.length);

        matches = ((StandardTitanGraph) graphdb).indexRetrieval(realName, name, itx);
        Assert.assertEquals(0, matches.length);

        clopen();

        itx = (InternalTitanTransaction) graphdb.newTransaction();
        stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();
        fx = new TitanGraphTools((StandardTitanGraph) graphdb);
        fx.repairType("name");

        clopen();

        //check that the index is repaird
        itx = (InternalTitanTransaction) graphdb.newTransaction();
        stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();


        //check that the index is busted
        matches = ((StandardTitanGraph) graphdb).indexRetrieval(fakeName, name, itx);
        Assert.assertEquals(0, matches.length);

        matches = ((StandardTitanGraph) graphdb).indexRetrieval(realName, name, itx);
        Assert.assertEquals(1, matches.length);

    }

    /**
     * Tests that the reindex operation fills in any missing data
     *
     * @throws Exception
     */
    @Test
    public void testReindexAddsMissingEntries() throws Exception {
        TitanKey name = makeType("name", String.class, true, false);
        tx.commit();

        int numVertices = 10;
        tx = graphdb.newTransaction();
        TitanVertex[] vertices = new TitanVertex[numVertices];
        TitanProperty[] properties = new TitanProperty[numVertices];
        for (int i=0; i<numVertices; i++) {
            TitanVertex v = tx.addVertex();
            TitanProperty p = v.addProperty("name", "name-" + (i%2));
            vertices[i] = v;
            properties[i] = p;
        }
        clopen();

        //first, check that the expected number of results are returned from the index
        InternalTitanTransaction itx;
        StoreTransaction stx;

        String corruptName = "name-0";

        itx = (InternalTitanTransaction) graphdb.newTransaction();
        stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();

        long[] matches;
        matches = ((StandardTitanGraph) graphdb).indexRetrieval(corruptName, name, itx);
        Assert.assertEquals(5, matches.length);

        TitanGraphTools fx = new TitanGraphTools((StandardTitanGraph) graphdb);

        //get the index columns to delete
        ByteBuffer key = fx.getIndexKey(corruptName);
        ArrayList<ByteBuffer> removeColumns = new ArrayList<ByteBuffer>();

        KeyColumnValueStore indexStore = fx.getBackend().getVertexIndexStore();
        ByteBuffer startCol = VariableLong.positiveByteBuffer(name.getID());
        List<Entry> columns = indexStore.getSlice(
                key,
                startCol,
                ByteBufferUtil.nextBiggerBuffer(startCol),
                stx
        );

        for (Entry entry: columns) {
            removeColumns.add(entry.getColumn());
        }

        //delete the columns
        indexStore.mutate(key, null, removeColumns, stx);
        itx.commit();

        clopen();

        //now, check that nothing is returned when querying the index
        itx = (InternalTitanTransaction) graphdb.newTransaction();
        matches = ((StandardTitanGraph) graphdb).indexRetrieval(corruptName, name, itx);
        Assert.assertEquals(0, matches.length);

        //reindex
        fx.reindexType(name);

        clopen();

        //check that the vertices are back
        itx = (InternalTitanTransaction) graphdb.newTransaction();
        matches = ((StandardTitanGraph) graphdb).indexRetrieval(corruptName, name, itx);
        Assert.assertEquals(5, matches.length);

    }
}
