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
import io.hetu.core.heuristicindex.PartitionIndexWriter;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.heuristicindex.SerializationUtils;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import org.apache.commons.compress.utils.IOUtils;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.hetu.core.heuristicindex.util.IndexServiceUtils.getSerializer;
import static io.prestosql.spi.heuristicindex.TypeUtils.extractValueFromRowExpression;

public class BTreeIndex
        implements Index
{
    public static final String ID = "BTREE";
    public static final String FILE_NAME = "index.bt";
    private static final String KEY_TYPE = "__hetu__keytype";
    private static final String VALUE_TYPE = "__hetu__valuetype";

    protected Map<String, String> symbolTable;
    protected BTreeMap<Object, String> dataMap;
    protected AtomicBoolean isDBCreated = new AtomicBoolean(false);
    protected BTreeMap<String, String> properties;
    protected DB db;
    protected File file;
    protected Set<kotlin.Pair<? extends Comparable<?>, String>> source;
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
    public Set<CreateIndexMetadata.Level> getSupportedIndexLevels()
    {
        return Sets.newHashSet(CreateIndexMetadata.Level.PARTITION, CreateIndexMetadata.Level.TABLE);
    }

    @Override
    public String getId()
    {
        return ID;
    }

    @Override
    public boolean addValues(List<Pair<String, List<Object>>> values)
    {
        throw new UnsupportedOperationException("AddValues is not supported for BTree. Use addKeyValues()");
    }

    @Override
    public void addKeyValues(List<Pair<String, List<Pair<Comparable<? extends Comparable<?>>, String>>>> input)
    {
        if (!isDBCreated.get()) {
            setupDB();
        }
        if (source == null) {
            keyType = IndexServiceUtils.extractType(input.get(0).getSecond().get(0).getFirst());
            valueType = IndexServiceUtils.extractType(input.get(0).getSecond().get(0).getSecond());
            source = new TreeSet<>((o1, o2) -> {
                if (input.get(0).getSecond().get(0).getFirst() instanceof Comparable) {
                    return ((Comparable) o1.getFirst()).compareTo(o2.getFirst());
                }
                throw new RuntimeException("Type is not supported");
            });
        }
        if (input.size() == 1) {
            for (Pair<Comparable<? extends Comparable<?>>, String> pair : input.get(0).getSecond()) {
                source.add(new kotlin.Pair<Comparable<? extends Comparable<?>>, String>(pair.getFirst(), pair.getSecond()));
            }
        }
        else {
            throw new UnsupportedOperationException("Composite B Tree index is not supported");
        }
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
    public Iterator<String> lookUp(Object expression)
    {
        List<String> result = new ArrayList<>();

        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            Object key = extractValueFromRowExpression(callExp.getArguments().get(1));
            Optional<OperatorType> operatorOptional = Signature.getOperatorType(((CallExpression) expression).getSignature().getName());
            if (operatorOptional.isPresent()) {
                OperatorType operator = operatorOptional.get();
                switch (operator) {
                    case EQUAL:
                        if (dataMap.containsKey(key)) {
                            result.addAll(translateSymbols(dataMap.get(key)));
                        }
                        break;
                    case LESS_THAN:
                        ConcurrentNavigableMap<Object, String> concurrentNavigableMap = dataMap.subMap(dataMap.firstKey(), true, key, false);
                        result.addAll(concurrentNavigableMap.values().stream().map(this::translateSymbols).flatMap(Collection::stream).collect(Collectors.toList()));
                        break;
                    case LESS_THAN_OR_EQUAL:
                        concurrentNavigableMap = dataMap.subMap(dataMap.firstKey(), true, key, true);
                        result.addAll(concurrentNavigableMap.values().stream().map(this::translateSymbols).flatMap(Collection::stream).collect(Collectors.toList()));
                        break;
                    case GREATER_THAN:
                        concurrentNavigableMap = dataMap.subMap(key, false, dataMap.lastKey(), true);
                        result.addAll(concurrentNavigableMap.values().stream().map(this::translateSymbols).flatMap(Collection::stream).collect(Collectors.toList()));
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        concurrentNavigableMap = dataMap.subMap(key, true, dataMap.lastKey(), true);
                        result.addAll(concurrentNavigableMap.values().stream().map(this::translateSymbols).flatMap(Collection::stream).collect(Collectors.toList()));
                        break;
                    default:
                        throw new UnsupportedOperationException("Expression not supported");
                }
            }
        }
        else if (expression instanceof SpecialForm) {
            SpecialForm specialForm = (SpecialForm) expression;
            switch (specialForm.getForm()) {
                case BETWEEN:
                    Object left = extractValueFromRowExpression(specialForm.getArguments().get(1));
                    Object right = extractValueFromRowExpression(specialForm.getArguments().get(2));
                    ConcurrentNavigableMap<Object, String> concurrentNavigableMap = dataMap.subMap(left, true, right, true);
                    result.addAll(concurrentNavigableMap.values().stream().map(this::translateSymbols).flatMap(Collection::stream).collect(Collectors.toList()));
                    break;
                case IN:
                    for (RowExpression exp : specialForm.getArguments().subList(1, specialForm.getArguments().size())) {
                        Object key = extractValueFromRowExpression(exp);
                        if (dataMap.containsKey(key)) {
                            result.addAll(translateSymbols(dataMap.get(key)));
                        }
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Expression not supported");
            }
        }
        else {
            throw new UnsupportedOperationException("Expression not supported");
        }

        result.sort(String::compareTo);
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
        Properties properties = getProperties();
        if (properties.getProperty(PartitionIndexWriter.SYMBOL_TABLE_KEY_NAME) != null) {
            this.symbolTable = SerializationUtils.deserializeMap(properties.getProperty(PartitionIndexWriter.SYMBOL_TABLE_KEY_NAME), s -> s, s -> s);
        }
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

    private List<String> translateSymbols(String dataMapLookUpRes)
    {
        return Arrays.stream(dataMapLookUpRes.split(",")).map(res -> symbolTable != null ? symbolTable.get(res) : res).collect(Collectors.toList());
    }
}
