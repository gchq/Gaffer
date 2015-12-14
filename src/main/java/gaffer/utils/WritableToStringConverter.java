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

import org.apache.accumulo.core.util.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * Contains methods to serialise a {@link Writable} to a {@link String}, and vice-versa.
 */
public class WritableToStringConverter {

    private WritableToStringConverter() { }

    public static String serialiseToString(Writable writable) throws IOException {
        // Serialise to a byte array
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        Text.writeString(out, writable.getClass().getName());
        writable.write(out);

        // Convert the byte array to a base64 string
        return Base64.encodeBase64String(baos.toByteArray());
    }

    public static Writable deserialiseFromString(String serialised) throws IOException {
        // Convert the base64 string to a byte array
        byte[] b = Base64.decodeBase64(serialised);

        // Deserialise the writable from the byte array
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        DataInput in = new DataInputStream(bais);
        String className = Text.readString(in);
        try {
            Writable writable = (Writable) Class.forName(className).newInstance();
            writable.readFields(in);
            return writable;
        } catch (InstantiationException e) {
            throw new IOException("Exception deserialising writable: " + e);
        } catch (IllegalAccessException e) {
            throw new IOException("Exception deserialising writable: " + e);
        } catch (ClassNotFoundException e) {
            throw new IOException("Exception deserialising writable: " + e);
        } catch (ClassCastException e) {
            throw new IOException("Exception deserialising writable: " + e);
        }
    }
}
