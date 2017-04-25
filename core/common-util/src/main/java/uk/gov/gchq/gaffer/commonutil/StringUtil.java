/*
 * Copyright 2017 Crown Copyright
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
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

public final class StringUtil {

    public static final char COMMA = ',';
    private static final char ESCAPE_CHAR = '\\';
    private static final char REPLACEMENT_CHAR = ';';

    private StringUtil() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    public static String toString(final byte[] bytes) {
        try {
            return new String(bytes, CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to convert bytes to string", e);
        }
    }

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
            if (c == ESCAPE_CHAR) {
                escapedStr.append(ESCAPE_CHAR);
                escapedStr.append(REPLACEMENT_CHAR);
            } else if (c == COMMA) {
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
     * arrays that have been through the <code>escape</code> method.
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
                if (c == REPLACEMENT_CHAR) {
                    str.append(ESCAPE_CHAR);
                } else if (c == ESCAPE_CHAR) {
                    str.append(COMMA);
                } else {
                    str.append(c);
                }
                isEscaped = false;
            } else {
                if (c == ESCAPE_CHAR) {
                    isEscaped = true;
                } else {
                    str.append(c);
                }
            }
        }
        return str.toString();
    }

    public static byte[] toCsv(final Class<?>... classes) {
        final String[] classNames = new String[classes.length];
        for (int i = 0; i < classes.length; i++) {
            classNames[i] = classes[i].getName();
        }
        return toBytes(StringUtils.join(classNames, ","));
    }

    public static <T> Set<Class<? extends T>> csvToClasses(final byte[] bytes, final Class<? extends T> clazz) {
        final String[] classNames = toString(bytes).split(",");
        final Set<Class<? extends T>> classes = new HashSet<>(classNames.length);
        for (final String processorClassName : classNames) {
            try {
                classes.add(Class.forName(processorClassName).asSubclass(clazz));
            } catch (final ClassNotFoundException e) {
                throw new RuntimeException("Invalid class: " + processorClassName
                        + ". Should be an implementation of " + clazz.getName(), e);
            }
        }

        return classes;
    }
}
