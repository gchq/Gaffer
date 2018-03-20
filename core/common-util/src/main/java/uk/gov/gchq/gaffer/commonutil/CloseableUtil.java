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

package uk.gov.gchq.gaffer.commonutil;

/**
 * Utility class for handling {@link java.io.Closeable}s.
 */
public final class CloseableUtil {

    private CloseableUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Close a group of objects.
     * If an object implements {@link AutoCloseable} then the object will be closed.
     * Any exceptions that are thrown are caught and ignored.
     *
     * @param objs the objects to attempt to close
     */
    public static void close(final Object... objs) {
        for (final Object obj : objs) {
            close(obj);
        }
    }

    /**
     * Close an object.
     * If the object implements {@link AutoCloseable} then the object will be closed.
     * Any exceptions that are thrown are caught and ignored.
     *
     * @param obj the object to attempt to close
     */
    public static void close(final Object obj) {
        if (obj instanceof AutoCloseable) {
            close((AutoCloseable) obj);
        }
    }

    /**
     * Close a group of {@link AutoCloseable} objects.
     * Any exceptions that are thrown are caught and ignored.
     *
     * @param closeable the objects to close
     */
    public static void close(final AutoCloseable... closeable) {
        for (final AutoCloseable autoCloseable : closeable) {
            close(autoCloseable);
        }
    }

    /**
     * Close an {@link AutoCloseable} object.
     * Any exceptions that are thrown are caught and ignored.
     *
     * @param closeable the object to close
     */
    public static void close(final AutoCloseable closeable) {
        try {
            if (null != closeable) {
                closeable.close();
            }
        } catch (final Exception e) {
            // Ignore exception
        }
    }
}
