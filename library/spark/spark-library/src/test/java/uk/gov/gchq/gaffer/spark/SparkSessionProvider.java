/*
 * Copyright 2017-2019 Crown Copyright
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

public final class SparkSessionProvider {
    private static SparkSession sparkSession;

    private SparkSessionProvider() {
        //private constructor to prevent instantiation
    }

    public static synchronized SparkSession getSparkSession() {
        if (null == sparkSession) {
            sparkSession = SparkSession.builder()
                    .master("local")
                    .appName("spark-library-tests")
                    .config(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER)
                    .config(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR)
                    .getOrCreate();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> sparkSession.stop()));
        }
        return sparkSession;
    }
}
