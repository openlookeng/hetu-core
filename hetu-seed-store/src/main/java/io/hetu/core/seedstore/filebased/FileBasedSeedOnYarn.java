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

package io.hetu.core.seedstore.filebased;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.hetu.core.common.util.SecureObjectInputStream;
import io.prestosql.spi.seedstore.Seed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Objects;

/**
 * FileBasedSeedOnYarn is used for storing seed store on yarn information
 *
 * @since 2020-03-08
 */

public class FileBasedSeedOnYarn
        implements Seed
{
    private static final long serialVersionUID = 4L;

    // External URI (for instance, http://ip:8080)
    private String location;
    // Timestamp for this seed
    private long timestamp;
    // Hazelcast state store URI (for instance, ip:5701)
    private String internalStateStoreUri;

    @JsonCreator
    public FileBasedSeedOnYarn(
            @JsonProperty("location") String location,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("internal-state-store-uri") String internalStateStoreUri)
    {
        this.location = location;
        this.timestamp = timestamp;
        this.internalStateStoreUri = internalStateStoreUri;
    }

    /**
     * Deserialize FileBasedSeedOnyarn serialized string to FileBasedSeedOnyarn Object
     *
     * @param serialized FileBasedSeedOnyarn serialized String to be converted
     * @return FileBasedSeedOnyarn Object
     * @throws IOException if an I/O error occurs while reading string
     * @throws ClassNotFoundException if serialized string cannot be deserialized to FileBasedSeedOnyarn Object
     */
    public static FileBasedSeedOnYarn deserialize(String serialized)
            throws IOException, ClassNotFoundException
    {
        byte[] datas = Base64.getDecoder().decode(serialized);
        try (ObjectInputStream ois = new SecureObjectInputStream(new ByteArrayInputStream(datas),
                FileBasedSeedOnYarn.class.getName())) {
            FileBasedSeedOnYarn obj = (FileBasedSeedOnYarn) ois.readObject();
            return obj;
        }
    }

    @Override
    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @Override
    @JsonProperty
    public void setLocation(String location)
    {
        this.location = location;
    }

    @Override
    @JsonProperty
    public long getTimestamp()
    {
        return timestamp;
    }

    @Override
    @JsonProperty
    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    @JsonProperty
    public String getInternalStateStoreUri()
    {
        return internalStateStoreUri;
    }

    @Override
    public String getUniqueInstanceId()
    {
        return this.internalStateStoreUri;
    }

    @JsonProperty
    public void setInternalStateStoreUri(String internalStateStoreUri)
    {
        this.internalStateStoreUri = internalStateStoreUri;
    }

    @Override
    public String serialize()
            throws IOException
    {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(this);
            return Base64.getEncoder().encodeToString(baos.toByteArray());
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FileBasedSeedOnYarn fileBasedSeed = (FileBasedSeedOnYarn) obj;
        return location.equals(fileBasedSeed.location) && internalStateStoreUri.equals(fileBasedSeed.internalStateStoreUri);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(location);
    }

    @Override
    public String toString()
    {
        return "FileBasedSeedOnyarn{"
                + "location='" + location + '\''
                + ", timestamp=" + timestamp + '\''
                + ", internalStateStoreUri='" + internalStateStoreUri + "\'}";
    }
}
