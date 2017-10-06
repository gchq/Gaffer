/*
 * Copyright 2016-2017 Crown Copyright
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
import uk.gov.gchq.gaffer.store.ContextInitialiser;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

/**
 * A {@code SparkContextInitialiser} is a {@link ContextInitialiser} that adds
 * a {@link SparkSession} to the user {@link Context}.
 */
public class SparkContextInitialiser implements ContextInitialiser {
    public static final String SPARK_CONTEXT_CONFIG_KEY = "config.spark.context";

    @Override
    public void initialise(final Context context, final StoreProperties storeProperties) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(storeProperties.get(SparkConstants.APP_NAME, SparkConstants.DEFAULT_APP_NAME));
        if (Boolean.parseBoolean(storeProperties.get(SparkConstants.USE_SPARK_DEFAULT_CONF, "false"))) {
            Properties properties = new Properties();
            String sparkDefaultConfPath = storeProperties.get(SparkConstants.SPARK_DEFAULT_CONF_PATH, SparkConstants.DEFAULT_SPARK_DEFAULT_CONF_PATH);
            try {
                properties.load(Files.newBufferedReader(Paths.get(sparkDefaultConfPath)));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Failed to read spark-default conf from " + sparkDefaultConfPath, e);
            }
            for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
                builder.config((String) entry.getKey(), (String) entry.getValue());
            }
            String sparkMaster = storeProperties.get(SparkConstants.MASTER);
            builder = (sparkMaster == null || sparkMaster.isEmpty()) ? builder : builder.master(sparkMaster);
        } else {
            builder.master(storeProperties.get(SparkConstants.MASTER, SparkConstants.MASTER_DEFAULT));
        }
        builder.config(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, "true")
                .config(SparkConstants.SERIALIZER, storeProperties.get(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER))
                .config(SparkConstants.KRYO_REGISTRATOR, storeProperties.get(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR));
        context.setConfig(SPARK_CONTEXT_CONFIG_KEY, builder.getOrCreate());
    }

    /**
     * Create a new Context with the given user and spark session.
     *
     * @param user         the user
     * @param sparkSession the spark session
     * @return the new {@link Context}.
     */
    public static Context createContext(final User user, final SparkSession sparkSession) {
        return new Context.Builder()
                .user(user)
                .config(SPARK_CONTEXT_CONFIG_KEY, sparkSession)
                .build();
    }

    /**
     * Extracts the {@link SparkSession} from the Context. Throws an exception
     * if the session is null. This method will never return null.
     *
     * @param context the {@link User} {@link Context}
     * @return the {@link SparkSession}
     */
    public static SparkSession getSparkSession(final Context context) {
        final SparkSession sparkSession = (SparkSession) context.getConfig(SPARK_CONTEXT_CONFIG_KEY);
        if (null == sparkSession) {
            throw new IllegalArgumentException("The Context does not have a SparkSession.");
        }
        return sparkSession;
    }
}
