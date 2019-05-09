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
package uk.gov.gchq.gaffer.commonutil;

import org.junit.Test;

import java.io.InputStream;
import java.net.URI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class StreamUtilTest {

    public static final String FILE_NAME = "URLSchema.json";

    @Test
    public void testOpenStreamsURLNotEmpty() throws Exception {
        //Given
        final URI resource = getClass().getClassLoader().getResource(FILE_NAME).toURI();
        if (null == resource) {
            fail("Test json file not found:" + FILE_NAME);
        }

        //When
        final InputStream[] inputStreams = StreamUtil.openStreams(resource);

        //Then
        try {
            assertNotNull(inputStreams);
            assertFalse("InputStreams length is 0", 0 == inputStreams.length);
        } finally {
            StreamUtil.closeStreams(inputStreams);
        }
    }
}
