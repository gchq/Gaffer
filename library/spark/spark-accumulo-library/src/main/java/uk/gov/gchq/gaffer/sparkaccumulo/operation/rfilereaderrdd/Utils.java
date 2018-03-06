/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd;

import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class Utils {

    private Utils() {
        // Empty
    }

    public static byte[] serialiseConfiguration(final Configuration configuration) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final DataOutputStream dos = new DataOutputStream(baos);
            configuration.write(dos);
            final byte[] serialised = baos.toByteArray();
            baos.close();
            return serialised;
        }
    }

    public static Configuration deserialiseConfiguration(final byte[] serialisedConfiguration) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialisedConfiguration)) {
            final DataInputStream dis = new DataInputStream(bais);
            final Configuration configuration = new Configuration();
            configuration.readFields(dis);
            dis.close();
            return configuration;
        }
    }
}
