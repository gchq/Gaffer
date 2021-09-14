/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class StreamUtilTest {

    public static final String FILE_NAME = "URLSchema.json";

    @Test
    public void testOpenStreamsURLNotEmpty() throws Exception {
        final URI resource = getClass().getClassLoader().getResource(FILE_NAME).toURI();
        if (null == resource) {
            fail("Test json file not found:" + FILE_NAME);
        }

        final InputStream[] inputStreams = StreamUtil.openStreams(resource);

        assertThat(inputStreams)
                .isNotEmpty()
                .overridingErrorMessage("InputStreams length is %s", 0);

        StreamUtil.closeStreams(inputStreams);
    }
}
