/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hetu.core.plugin.heuristicindex.index.btree;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.hetu.core.heuristicindex.util.TypeUtils;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.KeyValue;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import kotlin.Pair;
import org.apache.commons.compress.utils.IOUtils;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.hetu.core.heuristicindex.util.TypeUtils.extractSingleValue;
import static io.hetu.core.heuristicindex.util.TypeUtils.getComparator;

public class BTreeIndex
        implements Index
{
    public static final String ID = "BTREE";
    public static final String FILE_NAME = "index.bt";
    private static final String KEY_TYPE = "__hetu__keytype";
    private static final String VALUE_TYPE = "__hetu__valuetype";

    protected BTreeMap dataMap;
    protected AtomicBoolean isDBCreated = new AtomicBoolean(false);
    protected BTreeMap<String, String> properties;
    protected DB db;
    protected File file;
    protected Set<Pair> source;
    protected String keyType;
    protected String valueType;

    public BTreeIndex()
    {
        file = new File(Files.createTempDir() + "/btree-" + UUID.randomUUID().toString());
    }

    private synchronized void setupDB()
    {
        if (!isDBCreated.get()) {
            db = DBMaker
                    .fileDB(file)
                    .fileMmapEnableIfSupported()
                    .cleanerHackEnable()
                    .make();
            properties = db.treeMap("propertiesMap")
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(Serializer.STRING)
                    .createOrOpen();
            if (properties.containsKey(KEY_TYPE)) {
                createDBMap(properties.get(KEY_TYPE), properties.get(VALUE_TYPE));
            }
            isDBCreated.compareAndSet(false, true);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    close();
                }
                catch (IOException e) {
                    // Do nothing
                }
            }));
        }
    }

    private GroupSerializer getSerializer(String type)
    {
        switch (type) {
            case "long":
            case "Long":
                return Serializer.LONG;
            case "Slice":
            case "String":
                return Serializer.STRING;
            case "int":
            case "Integer":
                return Serializer.INTEGER;
        }
        throw new RuntimeException("Index is not supported for type: (" + type + ")");
    }

    private synchronized void createBatchWriteDBMap(String keyType, String valueType)
    {
        if (dataMap == null) {
            dataMap = db.treeMap("dataMap")
                    .keySerializer(getSerializer(keyType))
                    .valueSerializer(new SnappyCompressionSerializer(getSerializer(valueType)))
                    .createFrom(source.iterator());
            properties.put(KEY_TYPE, keyType);
            properties.put(VALUE_TYPE, valueType);
        }
    }

    private synchronized void createDBMap(String keyType, String valueType)
    {
        if (dataMap == null) {
            dataMap = db.treeMap("dataMap")
                    .keySerializer(getSerializer(keyType))
                    .valueSerializer(new SnappyCompressionSerializer(getSerializer(valueType)))
                    .open();
            properties.put(KEY_TYPE, keyType);
            properties.put(VALUE_TYPE, valueType);
        }
    }

    @Override
    public Set<Level> getSupportedIndexLevels()
    {
        return Sets.newHashSet(Level.PARTITION);
    }

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public boolean addValues(List<io.prestosql.spi.heuristicindex.Pair<String, List<Object>>> values)
            throws IOException
    {
        throw new UnsupportedOperationException("AddValues is not supported for BTree. Use addKeyValues()");
    }

    @Override
    public boolean addKeyValues(List<io.prestosql.spi.heuristicindex.Pair<String, List<KeyValue>>> input)
            throws IOException
    {
        if (!isDBCreated.get()) {
            setupDB();
        }
        if (source == null) {
            keyType = TypeUtils.extractType(input.get(0).getSecond().get(0).getKey());
            valueType = TypeUtils.extractType(input.get(0).getSecond().get(0).getValue());
            source = new TreeSet<>(getComparator(keyType));
        }
        if (input.size() == 1) {
            for (KeyValue keyValue : input.get(0).getSecond()) {
                source.add(new Pair(keyValue.getKey(), keyValue.getValue()));
            }
        }
        else {
            throw new UnsupportedOperationException("Composite B Tree index is not supported");
        }
        return true;
    }

    @Override
    public Properties getProperties()
    {
        Properties result = new Properties();
        for (Map.Entry<String, String> entry : this.properties.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    @Override
    public void setProperties(Properties properties)
    {
        Enumeration e = properties.propertyNames();
        while (e.hasMoreElements()) {
            Object key = e.nextElement();
            this.properties.put((String) key, properties.getProperty((String) key));
        }
    }

    @Override
    public boolean matches(Object expression)
    {
        return lookUp(expression).hasNext();
    }

    @Override
    public <I> Iterator<I> lookUp(Object expression)
    {
        List result = new ArrayList();

        if (expression instanceof ComparisonExpression) {
            ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
            Object key = extractSingleValue(comparisonExpression.getRight());
            switch (comparisonExpression.getOperator()) {
                case EQUAL:
                    if (dataMap.containsKey(key)) {
                        result.add(dataMap.get(key));
                    }
                    break;
                case LESS_THAN:
                    ConcurrentNavigableMap concurrentNavigableMap = dataMap.subMap(dataMap.firstKey(), true, key, false);
                    result.addAll(concurrentNavigableMap.values());
                    break;
                case LESS_THAN_OR_EQUAL:
                    concurrentNavigableMap = dataMap.subMap(dataMap.firstKey(), true, key, true);
                    result.addAll(concurrentNavigableMap.values());
                    break;
                case GREATER_THAN:
                    concurrentNavigableMap = dataMap.subMap(key, false, dataMap.lastKey(), true);
                    result.addAll(concurrentNavigableMap.values());
                    break;
                case GREATER_THAN_OR_EQUAL:
                    concurrentNavigableMap = dataMap.subMap(key, false, dataMap.lastKey(), true);
                    result.addAll(concurrentNavigableMap.values());
                    break;
            }
        }
        else if (expression instanceof BetweenPredicate) {
            BetweenPredicate betweenPredicate = (BetweenPredicate) expression;
            Object left = extractSingleValue(betweenPredicate.getMin());
            Object right = extractSingleValue(betweenPredicate.getMax());
            ConcurrentNavigableMap concurrentNavigableMap = dataMap.subMap(left, right);
            result.addAll(concurrentNavigableMap.values());
        }
        else if (expression instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) expression;
            InListExpression inListExpression = (InListExpression) inPredicate.getValueList();
            for (Expression value : inListExpression.getValues()) {
                Object key = extractSingleValue(value);
                if (dataMap.containsKey(key)) {
                    result.add(dataMap.get(key));
                }
            }
        }
        else {
            throw new UnsupportedOperationException("Expression not supported");
        }
        return result.iterator();
    }

    @Override
    public void serialize(OutputStream out)
            throws IOException
    {
        createBatchWriteDBMap(keyType, valueType);

        if (!db.isClosed()) {
            db.commit();
            dataMap.close();
            db.close();
        }

        try (InputStream inputStream = new FileInputStream(file); SnappyOutputStream sout = new SnappyOutputStream(out)) {
            IOUtils.copy(inputStream, sout);
        }
    }

    @Override
    public Index deserialize(InputStream in)
            throws IOException
    {
        try (OutputStream out = new FileOutputStream(file)) {
            IOUtils.copy(new SnappyInputStream(in), out);
        }
        setupDB();
        return this;
    }

    @Override
    public void close()
            throws IOException
    {
        if (db != null) {
            db.close();
        }

        String parentDir = file.getParent();
        file.delete();
        java.nio.file.Files.deleteIfExists(Paths.get(parentDir));
    }
}
