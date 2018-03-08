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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class AbstractGetRDDHandlerTest {

    @Test
    public void testConvertingConfigurationToString() throws IOException {
        // Given
        final Configuration conf = new Configuration();
        conf.set("option1", "value1");
        conf.set("option2", "value2");

        // When
        final String encodedConf = AbstractGetRDDHandler.convertConfigurationToString(conf);
        final Configuration decodedConf = AbstractGetRDDHandler.convertStringToConfiguration(encodedConf);

        // Then
        assertEquals(conf.get("option1"), decodedConf.get("option1"));
        assertEquals(conf.get("option2"), decodedConf.get("option2"));
    }
}
