/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.utils;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Unit test of {@link WritableToStringConverter}.
 */
public class TestWritableToStringConverter {

    @Test
    public void test() throws IOException {
        IntWritable iw = new IntWritable(1);
        String serialised = WritableToStringConverter.serialiseToString(iw);
        IntWritable read = (IntWritable) WritableToStringConverter.deserialiseFromString(serialised);
        assertEquals(iw, read);
    }

}
