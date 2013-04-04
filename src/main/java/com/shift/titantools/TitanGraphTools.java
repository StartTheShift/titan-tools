package com.shift.titantools;

import com.shift.titantools.RepairException;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.types.manager.TypeManager;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

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

    public ByteBuffer getIndexValue(TitanProperty prop) throws RepairException {
        try {
            return (ByteBuffer) getIndexValueMethod.invoke(graph, prop);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        } catch (InvocationTargetException e) {
            throw new RepairException(e);
        }
    }

    public ByteBuffer getKeyedIndexColumn(TitanKey type) throws RepairException {
        try {
            return (ByteBuffer) getKeyedIndexColumnMethod.invoke(graph, type);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        } catch (InvocationTargetException e) {
            throw new RepairException(e);
        }
    }

    public ByteBuffer getIndexColumn(TitanKey type, long propertyID) throws RepairException {
        try {
            return (ByteBuffer) getIndexColumnMethod.invoke(graph, type, propertyID);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        } catch (InvocationTargetException e) {
            throw new RepairException(e);
        }
    }

    public Backend getBackend() throws RepairException {
        try {
            return (Backend) backendField.get(graph);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        }
    }

    public TypeManager getTypeManager() throws RepairException {
        try {
            return (TypeManager) typeManagerField.get(graph);
        } catch (IllegalAccessException e) {
            throw new RepairException(e);
        }
    }
}
