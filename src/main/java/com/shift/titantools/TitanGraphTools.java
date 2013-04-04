package com.shift.titantools;

import com.sun.xml.internal.ws.util.QNameMap;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.RecordIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;
import com.thinkaurelius.titan.graphdb.types.manager.TypeManager;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Wraps a Titan graph and exposes various utility methods
 */
public class TitanGraphTools {
    private StandardTitanGraph graph;
    private Method getIndexKeyMethod;
    private Method getIndexValueMethod;
    private Method getKeyedIndexColumnMethod;
    private Method getIndexColumnMethod;

    private Field backendField;
    private Field typeManagerField;

    public TitanGraphTools(StandardTitanGraph graph) throws RepairException {
        this.graph = graph;
        getPrivateMethods();
    }

    /**
     * pulls out the private graph methods we need for index repair and testing, and makes them
     * publicly accessible
     *
     * @throws RepairException
     */
    private void getPrivateMethods() throws RepairException {
        try {
            getIndexKeyMethod = graph.getClass().getDeclaredMethod("getIndexKey", Object.class);
            getIndexKeyMethod.setAccessible(true);

            getIndexValueMethod = graph.getClass().getDeclaredMethod("getIndexValue", TitanProperty.class);
            getIndexValueMethod.setAccessible(true);

            getKeyedIndexColumnMethod = graph.getClass().getDeclaredMethod("getKeyedIndexColumn", TitanKey.class);
            getKeyedIndexColumnMethod.setAccessible(true);

            getIndexColumnMethod = graph.getClass().getDeclaredMethod("getIndexColumn", TitanKey.class, long.class);
            getIndexColumnMethod.setAccessible(true);

            backendField = graph.getClass().getDeclaredField("backend");
            backendField.setAccessible(true);

            typeManagerField = graph.getClass().getDeclaredField("etManager");
            typeManagerField.setAccessible(true);

        } catch (NoSuchFieldException e) {
            throw new RepairException(e);
        } catch (NoSuchMethodException e) {
            throw new RepairException(e);
        }
    }

    /**
     * Calls the getIndexKey method on the graph object
     *
     * @param att
     * @return
     * @throws com.shift.titantools.RepairException
     */
    public ByteBuffer getIndexKey(Object att) throws RepairException {
        try {
            return (ByteBuffer) getIndexKeyMethod.invoke(graph, att);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        } catch (InvocationTargetException e) {
            throw new RepairException(e);
        }
    }

    /**
     * Calls the getIndexValue method on the graph object
     *
     * @param prop
     * @return
     * @throws com.shift.titantools.RepairException
     */
    public ByteBuffer getIndexValue(TitanProperty prop) throws RepairException {
        try {
            return (ByteBuffer) getIndexValueMethod.invoke(graph, prop);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        } catch (InvocationTargetException e) {
            throw new RepairException(e);
        }
    }

    /**
     * Calls the getKeyedIndexColumn method on the graph object
     *
     * @param type
     * @return
     * @throws com.shift.titantools.RepairException
     */
    public ByteBuffer getKeyedIndexColumn(TitanKey type) throws RepairException {
        try {
            return (ByteBuffer) getKeyedIndexColumnMethod.invoke(graph, type);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        } catch (InvocationTargetException e) {
            throw new RepairException(e);
        }
    }

    /**
     * Calls the getIndexColumn method on the graph object
     *
     * @param type
     * @param propertyID
     * @return
     * @throws RepairException
     */
    public ByteBuffer getIndexColumn(TitanKey type, long propertyID) throws RepairException {
        try {
            return (ByteBuffer) getIndexColumnMethod.invoke(graph, type, propertyID);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        } catch (InvocationTargetException e) {
            throw new RepairException(e);
        }
    }

    /**
     * Returns the backend instance of the wrapped graph
     *
     * @return
     * @throws RepairException
     */
    public Backend getBackend() throws RepairException {
        try {
            return (Backend) backendField.get(graph);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        }
    }

    /**
     * Returns the type manager instance of the wrapped graph
     *
     * @return
     * @throws RepairException
     */
    public TypeManager getTypeManager() throws RepairException {
        try {
            return (TypeManager) typeManagerField.get(graph);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        }
    }

    /**
     * Performs diagnostics on indices, optionally repairing any problems it finds
     *
     * @param type
     * @param repair
     * @return
     * @throws RepairException
     */
    protected int processType(TitanType type, boolean repair) throws RepairException {

        if (type.isPropertyKey()) {
            //begin graph and store transactions
            InternalTitanTransaction itx = (InternalTitanTransaction) graph.newTransaction();
            StoreTransaction stx = ((BackendTransaction) itx.getTxHandle()).getStoreTransactionHandle();

            //get the key
            TitanKey titanKey = itx.getPropertyKey(type.getName());

            KeyColumnValueStore indexStore = getBackend().getVertexIndexStore();
            try {

                //we need to iterate over all keys in the index
                RecordIterator<ByteBuffer> keys = indexStore.getKeys(stx);
                while (keys.hasNext()) {
                    ByteBuffer key = keys.next();
                    List<ByteBuffer> deletions = new ArrayList<ByteBuffer>();
                    ByteBuffer startCol = VariableLong.positiveByteBuffer(titanKey.getID());
                    List<Entry> columns = indexStore.getSlice(
                            key,
                            startCol,
                            ByteBufferUtil.nextBiggerBuffer(startCol),
                            stx
                    );
                    for (Entry entry : columns) {
                        long eid = VariableLong.readPositive(entry.getValue());
                        TitanVertex v = (TitanVertex) graph.getVertex(eid);
                        if (v == null) {
                            deletions.add(entry.getColumn());
                            System.out.println("deleted vertex found in index");
                        } else {
                            //verify that the given property matches
                            Object value = v.getProperty(titanKey.getName());
                            ByteBuffer indexKey = getIndexKey(value);
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
            } catch (StorageException e) {
                throw new RepairException(e);
            } finally {
                //cleanup
                itx.commit();
            }

            return 0;

        } else {
            throw new RepairException("the given type is not a property key");
        }
    }

    public void repairType(TitanType type) throws RepairException {
        processType(type, true);
    }

    public void repairType(String typeName) throws RepairException {
        TitanType type = graph.getType(typeName);
        repairType(type);
    }

    public void checkType(TitanType type) throws RepairException {
        processType(type, false);
    }

    public void checkType(String typeName) throws RepairException {
        TitanType type = graph.getType(typeName);
        checkType(type);
    }
}
