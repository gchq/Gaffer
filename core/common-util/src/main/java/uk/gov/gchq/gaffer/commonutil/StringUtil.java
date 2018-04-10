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

import org.apache.commons.lang3.StringUtils;

import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility methods for {@link String}s.
 */
public final class StringUtil {

    public static final char COMMA = ',';
    private static final char ESCAPE_CHAR = '\\';
    private static final char REPLACEMENT_CHAR = ';';

    private StringUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Create a string representation of a byte array.
     *
     * @param bytes the byte array to convert to a string representation
     * @return the resulting string
     */
    public static String toString(final byte[] bytes) {
        try {
            return new String(bytes, CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to convert bytes to string", e);
        }
    }

    /**
     * Create a byte array representation of a string.
     *
     * @param string the string to convert into a byte array representation
     * @return the resulting byte array
     */
    public static byte[] toBytes(final String string) {
        if (null == string) {
            return new byte[0];
        }
        try {
            return string.getBytes(CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to convert bytes to string", e);
        }
    }

    /**
     * Escapes the provided string so that it no longer contains the
     * COMMA character.
     *
     * @param str the string to escape
     * @return the escaped string
     */
    public static String escapeComma(final String str) {
        final StringBuilder escapedStr = new StringBuilder(str.length());
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (ESCAPE_CHAR == c) {
                escapedStr.append(ESCAPE_CHAR);
                escapedStr.append(REPLACEMENT_CHAR);
            } else if (COMMA == c) {
                escapedStr.append(ESCAPE_CHAR);
                escapedStr.append(ESCAPE_CHAR);
            } else {
                escapedStr.append(c);
            }
        }
        return escapedStr.toString();
    }

    /**
     * Unescapes the provided byte array - this should only be called on byte
     * arrays that have been through the {@code escape} method.
     *
     * @param escapedStr the escaped string
     * @return the unescaped string
     */
    public static String unescapeComma(final String escapedStr) {
        final StringBuilder str = new StringBuilder(escapedStr.length());
        boolean isEscaped = false;
        for (int i = 0; i < escapedStr.length(); i++) {
            char c = escapedStr.charAt(i);
            if (isEscaped) {
                if (REPLACEMENT_CHAR == c) {
                    str.append(ESCAPE_CHAR);
                } else if (ESCAPE_CHAR == c) {
                    str.append(COMMA);
                } else {
                    str.append(c);
                }
                isEscaped = false;
            } else {
                if (ESCAPE_CHAR == c) {
                    isEscaped = true;
                } else {
                    str.append(c);
                }
            }
        }
        return str.toString();
    }

    /**
     * Create a CSV entry containing a list of class names (in byte array representation).
     *
     * @param classes the classes to write as a CSV entry
     * @return the CSV entry, as a byte array
     */
    public static byte[] toCsv(final Class<?>... classes) {
        final String[] classNames = new String[classes.length];
        for (int i = 0; i < classes.length; i++) {
            classNames[i] = classes[i].getName();
        }
        return toBytes(StringUtils.join(classNames, ","));
    }

    /**
     * Convert a byte array containing a CSV entry of class names into a {@link Set}
     * of {@link Class} objects.
     * <p>
     * All the {@link Class} objects created are then cast to a subclass of the
     * type {@code T}.
     *
     * @param bytes the CSV entry
     * @param clazz the {@link Class} instance to cast to
     * @param <T>   the base type to cast all of the {@link Class} instances to a
     *              subtype of
     * @return a set of {@link Class} instances
     */
    public static <T> Set<Class<? extends T>> csvToClasses(final byte[] bytes, final Class<? extends T> clazz) {
        final String[] classNames = toString(bytes).split(",");
        final Set<Class<? extends T>> classes = new HashSet<>(classNames.length);
        for (final String className : classNames) {
            try {
                classes.add(Class.forName(SimpleClassNameIdResolver.getClassName(className)).asSubclass(clazz));
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException("Invalid class: " + className
                        + ". Should be an implementation of " + clazz.getName(), e);
            }
        }

        return classes;
    }
}
