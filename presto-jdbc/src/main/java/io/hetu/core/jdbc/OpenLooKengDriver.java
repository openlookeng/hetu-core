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
package io.hetu.core.jdbc;

import io.prestosql.client.SocketChannelSocketFactory;
import io.prestosql.jdbc.PrestoDriver;
import okhttp3.OkHttpClient;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.nullToEmpty;
import static io.prestosql.client.OkHttpUtil.userAgent;
import static java.lang.Integer.parseInt;

/**
 * openLooKeng Driver to be used with JDBC connection.
 *
 */
public class OpenLooKengDriver
        extends PrestoDriver
{
    public static final String DRIVER_NAME = "openLooKeng JDBC Driver";

    public static final String PRODUCT_NAME = "openLooKeng";

    public static final String DRIVER_VERSION;

    public static final int DRIVER_VERSION_MAJOR;

    public static final int DRIVER_VERSION_MINOR;

    public static final String DRIVER_PRODUCT_TAG = "lk:";

    public static final String DRIVER_URL_START = "jdbc:" + DRIVER_PRODUCT_TAG;

    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .addInterceptor(userAgent(DRIVER_NAME + "/" + DRIVER_VERSION))
            .socketFactory(new SocketChannelSocketFactory())
            .build();

    static {
        String version = nullToEmpty(OpenLooKengDriver.class.getPackage().getSpecificationVersion());
        Matcher matcher = Pattern.compile("^(\\d+)(\\.(\\d+))?($|[.-])").matcher(version);
        if (!matcher.find()) {
            DRIVER_VERSION = "unknown";
            DRIVER_VERSION_MAJOR = 0;
            DRIVER_VERSION_MINOR = 0;
        }
        else {
            final int thirdGroup = 3;
            DRIVER_VERSION = version;
            DRIVER_VERSION_MAJOR = parseInt(matcher.group(1));
            DRIVER_VERSION_MINOR = parseInt(firstNonNull(matcher.group(thirdGroup), "0"));
        }

        try {
            DriverManager.registerDriver(new OpenLooKengDriver());
        }
        catch (SQLException e) {
            throw new RuntimeException("Fail to register Jdbc driver for openLooKeng.");
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith(DRIVER_URL_START);
    }
}
