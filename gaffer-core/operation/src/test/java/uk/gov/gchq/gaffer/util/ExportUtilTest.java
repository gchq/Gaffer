/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;


public class ExportUtilTest {

    @Test
    public void shouldAcceptKeyThatContainsOnlyLowerAndUppercaseLetters() {
        checkKeyAccepted("aValidKey");
    }

    @Test
    public void shouldAcceptKeyThatContainsOnlyLowercaseLetters() {
        checkKeyAccepted("avalidkey");
    }

    @Test
    public void shouldAcceptKeyThatContainsOnlyUppercaseLetters() {
        checkKeyAccepted("AVALIDKEY");
    }

    @Test
    public void shouldRejectKeyThatContainsASlash() {
        checkKeyRejected("/keyWithASlash");
    }

    @Test
    public void shouldRejectKeyThatContainsALeadingFullStop() {
        checkKeyRejected(".keyWithFullStop");
    }

    @Test
    public void shouldRejectKeyThatContainsATrailingFullStop() {
        checkKeyRejected("keyWithFullStop.");
    }

    @Test
    public void shouldRejectKeyThatContainsHyphens() {
        checkKeyRejected("key-with-hyphens");
    }

    private void checkKeyAccepted(final String key) {
        // When
        ExportUtil.validateKey(key);

        // Then - no exceptions
    }

    private void checkKeyRejected(final String key) {
        try {
            // When
            ExportUtil.validateKey(key);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            // Then
            assertNotNull(e.getMessage());
        }
    }
}
