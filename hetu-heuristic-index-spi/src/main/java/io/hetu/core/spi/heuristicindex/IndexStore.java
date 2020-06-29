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

package io.hetu.core.spi.heuristicindex;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Properties;

/**
 * Class to be implemented by anything which is used to store/persist the indexes
 * For example you can store them on the local filesystem of the machine that hetu is running on or in an hdfs
 *
 * @since 2019-08-18
 */
public interface IndexStore
        extends Closeable
{
    /**
     * <pre>
     * Value that is set in config.properties detailing to store the indexes
     * (ie, hetu.filter.indexstore.uri=/tmp/hetu/indices-new/)
     * </pre>
     */
    String ROOT_URI_KEY = "uri";

    /**
     * <pre>
     * This value will be used in the global/catalog configs to specify the
     * IndexStore type.
     * </pre>
     *
     * @return id of the IndexStore
     */
    String getId();

    /**
     * <pre>
     * Writes the `content` to an OutputStream
     *
     * This method does not guarantee that the stream will be closed or left open.
     * The caller should close the stream.
     * </pre>
     *
     * @param content String content to be written
     * @param out     destination OutputStream
     * @throws IOException due to filesystem issues
     */
    default void write(String content, OutputStream out) throws IOException
    {
        try (BufferedWriter bout = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            bout.write(content);
        }
    }

    /**
     * Write content to item at the given uri.
     *
     * @param content        Content to be written
     * @param uri            Destination uri
     * @param isOverwritable Overwrite existing item if true
     * @throws IOException If filesystem error occurs
     */
    void write(String content, String uri, boolean isOverwritable) throws IOException;

    /**
     * <pre>
     * Delete the item at the given uri.
     *
     * Does nothing if item does not exist at the uri.
     *
     * </pre>
     *
     * @param uri Path to the item to be deleted
     * @return whether item was deleted successfully
     * @throws IOException if nothing exists at the uri
     */
    boolean delete(String uri) throws IOException;

    /**
     * Get OutputStream of the item at the given uri.
     *
     * @param uri            item uri
     * @param isOverwritable Overwrite existing item if true
     * @return the OutputStream of the item at the given uri
     * @throws IOException If file system error occurs
     */
    OutputStream getOutputStream(String uri, boolean isOverwritable) throws IOException;

    /**
     * Get InputStream of the item at the given uri
     *
     * @param uri item uri
     * @return InputStream of the item at the given uri
     * @throws IOException If file system error occurs
     */
    InputStream read(String uri) throws IOException;

    /**
     * Check if item exists
     *
     * @param uri item uri
     * @return true if item exists, false otherwise
     * @throws IOException If file system error occurs
     */
    boolean exists(String uri) throws IOException;

    /**
     * Move item from the current uri to new uri
     *
     * @param curUri Current uri of item
     * @param newUri New uri of item
     * @return true if successful
     * @throws IOException If file system error occurs
     */
    boolean renameTo(String curUri, String newUri) throws IOException;

    /**
     * Get last modification time of the item as milliseconds since epoch (January 1, 1970 UTC)
     *
     * @param uri item uri
     * @return last modification time of the item as milliseconds since epoch (January 1, 1970 UTC)
     * @throws IOException If a filesystem error occurs
     */
    long getLastModifiedTime(String uri) throws IOException;

    /**
     * Returns the uris of the children of the item at the given uri if any
     *
     * @param uri         item uri
     * @param isRecursive search for children recursively
     * @return uris of the children or empty collection if no children found
     * @throws IOException If file system error occurs
     */
    Collection<String> listChildren(String uri, boolean isRecursive) throws IOException;

    /**
     * <pre>
     * Gets the properties of the IndexStore.
     *
     * These properties may be used as configs for the IndexStore to connect
     * to the underlying index store. For example, HDFS configs.
     *
     * Each IndexStore determines how to use these properties.
     * </pre>
     *
     * @return Properties
     */
    default Properties getProperties()
    {
        return new Properties();
    }

    /**
     * <pre>
     * Sets the properties of the IndexStore.
     *
     * These properties may be used as configs for the IndexStore to connect
     * to the underlying index store. For example, HDFS configs.
     *
     * Each IndexStore determines how to use these properties.
     * </pre>
     *
     * @param properties Properties to be set
     */
    default void setProperties(Properties properties)
    {
    }

    /**
     * Release any resources that might be held by the IndexStore.
     * This is managed by hetu-index module, and client code should not call this method manually.
     */
    @Override
    default void close() throws IOException
    {
    }
}
