/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.heuristicindex.PartitionIndexWriter;
import io.hetu.core.heuristicindex.util.IndexServiceUtils;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexLookUpException;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.heuristicindex.SerializationUtils;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import org.apache.commons.compress.utils.IOUtils;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBException;
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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.hetu.core.heuristicindex.util.IndexServiceUtils.getSerializer;
import static io.prestosql.spi.heuristicindex.TypeUtils.extractValueFromRowExpression;

public class BTreeIndex
        implements Index
{
    public static final String ID = "BTREE";
    public static final String FILE_NAME = "index.bt";
    private static final String KEY_TYPE = "__hetu__keytype";
    private static final String VALUE_TYPE = "__hetu__valuetype";

    // when the lookup result is larger than this, processing lookUp result will take too long time and is not worthy
    private static final long TERMINATE_LOOKUP_SIZE_THRESHOLD = 10000L;
    // when the lookup result's weight in dataMap is larger than this, not much values can be filtered so filtering is not worth
    private static final double TERMINATE_LOOKUP_WEIGHT_THRESHOLD = 0.1;

    protected Map<String, String> symbolTable;
    protected BTreeMap<Object, String> dataMap;
    protected AtomicBoolean isDBCreated = new AtomicBoolean(false);
    protected BTreeMap<String, String> properties;
    protected DB db;
    protected TempFolder dataDir;
    protected File dataFile;
    protected Set<kotlin.Pair<? extends Comparable<?>, String>> source;
    protected String keyType;
    protected String valueType;

    public BTreeIndex()
    {
        dataDir = new TempFolder("btree");
        try {
            dataDir.create();
            dataFile = dataDir.getRoot().toPath().resolve("btree-" + UUID.randomUUID().toString()).toFile();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> dataDir.close()));
        }
        catch (IOException e) {
            dataDir.close();
            throw new UncheckedIOException("Failed to create temp directory for BTREE data", e);
        }
    }

    private synchronized void setupDB()
            throws IOException
    {
        if (!isDBCreated.get()) {
            try {
                db = DBMaker
                        .fileDB(dataFile)
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
            }
            catch (DBException dbe) {
                // rethrow IOException from DB
                if (dbe.getCause() instanceof IOException) {
                    throw (IOException) dbe.getCause();
                }
                else {
                    throw new IOException("Error setting up local mapdb: ", dbe);
                }
            }
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
            throws IOException
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
        try {
            return lookUp(expression).hasNext();
        }
        catch (IndexLookUpException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<String> lookUp(Object expression)
            throws IndexLookUpException
    {
        Collection<String> lookUpResults = Collections.emptyList();

        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            Object key = extractValueFromRowExpression(callExp.getArguments().get(1));
            BuiltInFunctionHandle builtInFunctionHandle;
            if (callExp.getFunctionHandle() instanceof BuiltInFunctionHandle) {
                builtInFunctionHandle = (BuiltInFunctionHandle) callExp.getFunctionHandle();
            }
            else {
                throw new UnsupportedOperationException("Unsupported function: " + callExp.getDisplayName());
            }
            Optional<OperatorType> operatorOptional = Signature.getOperatorType(builtInFunctionHandle.getSignature().getNameSuffix());
            if (operatorOptional.isPresent()) {
                OperatorType operator = operatorOptional.get();
                switch (operator) {
                    case EQUAL:
                        if (dataMap.containsKey(key)) {
                            lookUpResults = Collections.singleton(dataMap.get(key));
                        }
                        break;
                    case LESS_THAN:
                        lookUpResults = rangeLookUp(dataMap.firstKey(), true, key, false);
                        break;
                    case LESS_THAN_OR_EQUAL:
                        lookUpResults = rangeLookUp(dataMap.firstKey(), true, key, true);
                        break;
                    case GREATER_THAN:
                        lookUpResults = rangeLookUp(key, false, dataMap.lastKey(), true);
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        lookUpResults = rangeLookUp(key, true, dataMap.lastKey(), true);
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
                    lookUpResults = rangeLookUp(left, true, right, true);
                    break;
                case IN:
                    lookUpResults = new ArrayList<>();
                    for (RowExpression exp : specialForm.getArguments().subList(1, specialForm.getArguments().size())) {
                        Object key = extractValueFromRowExpression(exp);
                        if (dataMap.containsKey(key)) {
                            lookUpResults.add(dataMap.get(key));
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

        Set<String> symbolSet = new HashSet<>();
        // break lookUp results to symbols and keep in a set. e.g. ["1,2,2,4","2,3"] -> set{"1", "2", "3", "4"}
        for (String res : lookUpResults) {
            Collections.addAll(symbolSet, res.split(","));
        }

        // translate the symbols to actual data values
        List<String> translated = new ArrayList<>(symbolSet.size());
        for (String sym : symbolSet) {
            translated.add(symbolTable != null ? symbolTable.get(sym) : sym);
        }
        translated.sort(String::compareTo);
        return translated.iterator();
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

        try (InputStream inputStream = new FileInputStream(dataFile); SnappyOutputStream sout = new SnappyOutputStream(out)) {
            IOUtils.copy(inputStream, sout);
        }
    }

    @Override
    public Index deserialize(InputStream in)
            throws IOException
    {
        try (OutputStream out = new FileOutputStream(dataFile)) {
            IOUtils.copy(new SnappyInputStream(in), out);
        }
        setupDB();
        Properties localProperties = getProperties();
        if (localProperties.getProperty(PartitionIndexWriter.SYMBOL_TABLE_KEY_NAME) != null) {
            this.symbolTable = SerializationUtils.deserializeMap(localProperties.getProperty(PartitionIndexWriter.SYMBOL_TABLE_KEY_NAME), s -> s, s -> s);
        }
        return this;
    }

    @Override
    public long getDiskUsage()
    {
        return dataFile.length();
    }

    @Override
    public void close()
            throws IOException
    {
        if (db != null) {
            db.close();
        }

        dataDir.close();
    }

    private Collection<String> rangeLookUp(Object from, boolean fromInclusive, Object to, boolean toInclusive)
            throws IndexLookUpException
    {
        if (dataMap.getComparator().compare(from, to) > 0) {
            return Collections.emptyList();
        }

        Collection<String> values = dataMap.subMap(from, fromInclusive, to, toInclusive).values();
        if (values.size() > TERMINATE_LOOKUP_SIZE_THRESHOLD &&
                (double) values.size() / dataMap.size() >= TERMINATE_LOOKUP_WEIGHT_THRESHOLD) {
            throw new IndexLookUpException("Look-up returned too many matching values. Filtering will not be effective. Skipping.");
        }

        return values;
    }
}
