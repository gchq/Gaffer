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

package uk.gov.gchq.gaffer.commonutil;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * This test class is copied from org.apache.accumulo.core.util.TextUtilTest.
 */
public class TextUtilTest {

    @Test
    public void testGetBytes() {
        // Given
        String longMessage = "This is some text";
        Text longMessageText = new Text(longMessage);
        String smallerMessage = "a";
        Text smallerMessageText = new Text(smallerMessage);
        Text someText = new Text(longMessage);

        // When / Then
        assertTrue(someText.equals(longMessageText));
        someText.set(smallerMessageText);
        assertTrue(someText.getLength() != someText.getBytes().length);
        assertTrue(TextUtil.getBytes(someText).length == smallerMessage.length());
        assertTrue((new Text(TextUtil.getBytes(someText))).equals(smallerMessageText));
    }
}
