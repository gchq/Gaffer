/*
 * Copyright 2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.partitioner.serialisation;

import uk.gov.gchq.gaffer.parquetstore.partitioner.NegativeInfinityPartitionKey;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PartitionKey;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PositiveInfinityPartitionKey;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class PartitionKeySerialiser {
    private static final int NEGATIVE_INFINITY_FLAG = -1;
    private static final int POSITIVE_INFINITY_FLAG = 1;
    private static final int FINITE_KEY_FLAG = 0;

    public void write(final PartitionKey partitionKey, final DataOutputStream stream) throws IOException {
        if (partitionKey instanceof NegativeInfinityPartitionKey) {
            stream.writeInt(NEGATIVE_INFINITY_FLAG);
            return;
        }
        if (partitionKey instanceof PositiveInfinityPartitionKey) {
            stream.writeInt(POSITIVE_INFINITY_FLAG);
            return;
        }
        stream.writeInt(FINITE_KEY_FLAG);
        final Object[] partitionKeyObjects = partitionKey.getPartitionKey();
        stream.writeInt(partitionKeyObjects.length);
        for (int i = 0; i < partitionKeyObjects.length; i++) {
            writeObject(partitionKeyObjects[i], stream);
        }
    }

    public PartitionKey read(final DataInputStream stream) throws IOException {
        final int flag = stream.readInt();
        if (NEGATIVE_INFINITY_FLAG == flag) {
            return new NegativeInfinityPartitionKey();
        }
        if (POSITIVE_INFINITY_FLAG == flag) {
            return new PositiveInfinityPartitionKey();
        }
        if (FINITE_KEY_FLAG != flag) {
            throw new IOException("Unexpected flag of " + flag);
        }
        final int numKeys = stream.readInt();
        final Object[] key = new Object[numKeys];
        for (int i = 0; i < key.length; i++) {
            key[i] = readObject(stream);
        }
        return new PartitionKey(key);
    }


    private void writeObject(final Object value, final DataOutputStream stream) throws IOException {
        if (value instanceof Boolean) {
            stream.writeUTF("Boolean");
            stream.writeBoolean((boolean) value);
            return;
        }
        if (value instanceof Byte) {
            stream.writeUTF("Byte");
            stream.write((byte) value);
            return;
        }
        if (value instanceof Short) {
            stream.writeUTF("Short");
            stream.writeShort((short) value);
            return;
        }
        if (value instanceof Integer) {
            stream.writeUTF("Integer");
            stream.writeInt((int) value);
            return;
        }
        if (value instanceof Long) {
            stream.writeUTF("Long");
            stream.writeLong((long) value);
            return;
        }
        if (value instanceof Float) {
            stream.writeUTF("Float");
            stream.writeFloat((float) value);
            return;
        }
        if (value instanceof Double) {
            stream.writeUTF("Double");
            stream.writeDouble((double) value);
            return;
        }
        if (value instanceof String) {
            stream.writeUTF("String");
            stream.writeUTF((String) value);
            return;
        }
        if (value instanceof byte[]) {
            stream.writeUTF("byte[]");
            final byte[] bytes = (byte[]) value;
            stream.writeInt(bytes.length);
            stream.write(bytes);
            return;
        }
        throw new IOException("Cannot write object of class " + value.getClass());
    }

    private Object readObject(final DataInputStream stream) throws IOException {
        final String classType = stream.readUTF();
        Object value;
        switch (classType) {
            case "Boolean":
                value = stream.readBoolean();
                break;
            case "Byte":
                value = (byte) stream.read();
                break;
            case "Short":
                value = stream.readShort();
                break;
            case "Integer":
                value = stream.readInt();
                break;
            case "Long":
                value = stream.readLong();
                break;
            case "Float":
                value = stream.readFloat();
                break;
            case "Double":
                value = stream.readDouble();
                break;
            case "String":
                value = stream.readUTF();
                break;
            case "byte[]":
                final int length = stream.readInt();
                value = new byte[length];
                final int lengthRead = stream.read((byte[]) value);
                if (length != lengthRead) {
                    throw new IOException("Wrong number of bytes (" + lengthRead + ") read in reading byte array");
                }
                break;
            default:
                throw new IOException("Cannot read type of " + classType);
        }
        return value;
    }
}
