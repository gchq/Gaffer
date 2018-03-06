/*
 * Copyright 2016-2018 Crown Copyright
 *
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
package uk.gov.gchq.gaffer.spark;

import org.apache.spark.sql.SparkSession;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

/**
 * A {@code SparkContextUtil} is a utility class for adding and retrieving
 * a {@link SparkSession} to/from the user {@link Context}.
 */
public final class SparkContextUtil {
    public static final String SPARK_CONTEXT_CONFIG_KEY = "config.spark.context";

    private SparkContextUtil() {
    }

    /**
     * Adds a spark session to the given {@link Context}.
     *
     * @param context         the user context
     * @param storeProperties the store properties - used to create a spark session
     */
    public static void addSparkSession(final Context context, final StoreProperties storeProperties) {
        addSparkSession(context, createSparkSession(storeProperties));
    }

    /**
     * Adds a spark session to the given {@link Context}.
     *
     * @param context      the user context
     * @param sparkSession the spark session to add to the context
     */
    public static void addSparkSession(final Context context, final SparkSession sparkSession) {
        context.setConfig(SPARK_CONTEXT_CONFIG_KEY, sparkSession);
    }

    /**
     * Create a new Context with the given user and store properties
     *
     * @param user            the user
     * @param storeProperties the store properties
     * @return the new {@link Context}.
     */
    public static Context createContext(final User user, final StoreProperties storeProperties) {
        final Context context = new Context(user);
        addSparkSession(context, storeProperties);
        return context;
    }

    /**
     * Create a new Context with the given user and spark session.
     *
     * @param user         the user
     * @param sparkSession the spark session
     * @return the new {@link Context}.
     */
    public static Context createContext(final User user, final SparkSession sparkSession) {
        final Context context = new Context(user);
        addSparkSession(context, sparkSession);
        return context;
    }

    /**
     * Extracts the {@link SparkSession} from the Context. If there is no
     * SparkSession in the Context then a new SparkSession instance is created
     * and added to the context.
     *
     * @param context    the {@link User} {@link Context}
     * @param properties the store properties - used to create a spark session if required
     * @return the {@link SparkSession}
     */
    public static SparkSession getSparkSession(final Context context, final StoreProperties properties) {
        SparkSession sparkSession = (SparkSession) context.getConfig(SPARK_CONTEXT_CONFIG_KEY);
        if (null == sparkSession) {
            sparkSession = createSparkSession(properties);
            addSparkSession(context, sparkSession);
        }
        return sparkSession;
    }

    public static SparkSession createSparkSession(final StoreProperties storeProperties) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(storeProperties.get(SparkConstants.APP_NAME, SparkConstants.DEFAULT_APP_NAME));
        if (Boolean.parseBoolean(storeProperties.get(SparkConstants.USE_SPARK_DEFAULT_CONF, "false"))) {
            final Properties properties = new Properties();
            final String sparkDefaultConfPath = storeProperties.get(SparkConstants.SPARK_DEFAULT_CONF_PATH, SparkConstants.DEFAULT_SPARK_DEFAULT_CONF_PATH);
            try {
                properties.load(Files.newBufferedReader(Paths.get(sparkDefaultConfPath)));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Failed to read spark-default conf from " + sparkDefaultConfPath, e);
            }
            for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
                builder.config((String) entry.getKey(), (String) entry.getValue());
            }
            final String sparkMaster = storeProperties.get(SparkConstants.MASTER);
            builder = (sparkMaster == null || sparkMaster.isEmpty()) ? builder : builder.master(sparkMaster);
        } else {
            builder.master(storeProperties.get(SparkConstants.MASTER, SparkConstants.MASTER_DEFAULT));
        }
        builder.config(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, "true")
                .config(SparkConstants.SERIALIZER, storeProperties.get(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER))
                .config(SparkConstants.KRYO_REGISTRATOR, storeProperties.get(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR));
        return builder.getOrCreate();
    }
}
