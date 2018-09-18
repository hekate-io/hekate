package io.hekate.messaging;

import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.core.internal.util.ArgAssert;
import java.io.IOException;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Message meta-data.
 */
public class MessageMetaData {
    /**
     * Codec for {@link MessageMetaData} values.
     *
     * @param <T> Value type.
     */
    public interface MetaDataCodec<T> {
        /** Codec for values of {@code byte[]} type. */
        MetaDataCodec<byte[]> BYTES = new MetaDataCodec<byte[]>() {
            @Override
            public byte[] encode(byte[] value) {
                return value;
            }

            @Override
            public byte[] decode(byte[] bytes) {
                return bytes;
            }
        };

        /** Codec for values of {@code UTF-8}-encoded strings. */
        MetaDataCodec<String> TEXT = new MetaDataCodec<String>() {
            @Override
            public byte[] encode(String value) {
                return value.getBytes(UTF_8);
            }

            @Override
            public String decode(byte[] bytes) {
                return new String(bytes, UTF_8);
            }
        };

        /**
         * Encodes value.
         *
         * @param value Value.
         *
         * @return Bytes.
         */
        byte[] encode(T value);

        /**
         * Decodes value.
         *
         * @param bytes Bytes.
         *
         * @return Value.
         */
        T decode(byte[] bytes);
    }

    /**
     * Key of a {@link MessageMetaData}'s attribute.
     *
     * @param <T> Value type.
     */
    public static final class Key<T> {
        /** See {@link #name()}. */
        private final String name;

        /** Cached of {@link #name}'s bytes. */
        private final byte[] nameBytes;

        /** Value codec. */
        private final MetaDataCodec<T> codec;

        private Key(String name, MetaDataCodec<T> codec) {
            this.name = name;
            this.codec = codec;

            this.nameBytes = name.getBytes(UTF_8);
        }

        /**
         * Constructs a new key.
         *
         * <p>
         * For performance reasons it is highly recommended to cache and reuse such instances via globally accessible static fields (i.e.
         * store them as constants).
         * </p>
         *
         * @param name Key name (see {@link #name()}).
         * @param codec Codec to encode/decode values of this key.
         * @param <T> Value type.
         *
         * @return New key.
         */
        public static <T> Key<T> of(String name, MetaDataCodec<T> codec) {
            ArgAssert.notNull(name, "Name");
            ArgAssert.notNull(codec, "Codec");

            return new Key<>(name, codec);
        }

        /**
         * Returns the name of this key.
         *
         * @return Name of this key.
         */
        public String name() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** Initial capacity of key/value pairs. */
    private static final int INIT_CAPACITY = 4;

    /** Keys and values (even elements are keys, odd elements are values). */
    private byte[][] keyAndValue;

    /** Amount of key/value pairs that are stored in {@link #keyAndValue}. */
    private int size;

    /**
     * Constructs a new instance.
     */
    public MessageMetaData() {
        // No-op.
    }

    private MessageMetaData(int size, byte[][] keyAndValue) {
        this.size = size;
        this.keyAndValue = keyAndValue;
    }

    /**
     * Reads a {@link MessageMetaData} from the specified reader.
     *
     * @param in Reader.
     *
     * @return An instance if {@link MessageMetaData} that was decoded from the reader.
     *
     * @throws IOException if failed to read data.
     */
    public static MessageMetaData readFrom(DataReader in) throws IOException {
        int size = in.readVarInt();

        if (size > 0) {
            byte[][] keyAndValue = new byte[size][];

            for (int i = 0; i < size; i++) {
                byte[] bytes = new byte[in.readVarInt()];

                in.readFully(bytes);

                keyAndValue[i] = bytes;
            }

            return new MessageMetaData(size, keyAndValue);
        } else {
            return new MessageMetaData();
        }
    }

    /**
     * Writes the content of this instance into the specified writer.
     *
     * @param out Writer.
     *
     * @throws IOException if failed to write data.
     * @see MessageMetaData#readFrom(DataReader)
     */
    public void writeTo(DataWriter out) throws IOException {
        out.writeVarInt(size);

        for (int i = 0; i < size; i++) {
            byte[] bytes = keyAndValue[i];

            out.writeVarInt(bytes.length);
            out.write(bytes);
        }
    }

    /**
     * Returns the value of the specified key. Returns {@code null} if such key doesn't exist.
     *
     * @param key Key.
     * @param <T> Value type.
     *
     * @return Value for the specified key or {@code null}.
     */
    public <T> T get(Key<T> key) {
        ArgAssert.notNull(key, "Key");

        if (keyAndValue != null) {
            for (int i = 0; i < size; i += 2) {
                if (Arrays.equals(keyAndValue[i], key.nameBytes)) {
                    return key.codec.decode(keyAndValue[i + 1]);
                }
            }
        }

        return null;
    }

    /**
     * Sets the key/value pair. If key already exists then its value will be overwritten.
     *
     * @param key Key.
     * @param value Value.
     * @param <T> Value type.
     */
    public <T> void set(Key<T> key, T value) {
        ArgAssert.notNull(key, "Key");
        ArgAssert.notNull(value, "Value");

        if (!replace(key, value)) {
            ensureCapacity();

            // Add new entry.
            keyAndValue[size++] = key.nameBytes;
            keyAndValue[size++] = key.codec.encode(value);
        }
    }

    /**
     * Returns the amount of key/value pairs of this instance.
     *
     * @return Amount of key/value pairs of this instance.
     */
    public int size() {
        return size / 2;
    }

    /**
     * Returns {@code true} if this instance doesn't hold any key/value pairs.
     *
     * @return {@code true} if this instance doesn't hold any key/value pairs.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    private <T> boolean replace(Key<T> key, T value) {
        if (keyAndValue != null) {
            for (int i = 0; i < size; i += 2) {
                byte[] otherKeyBytes = keyAndValue[i];

                if (Arrays.equals(otherKeyBytes, key.nameBytes)) {
                    keyAndValue[i + 1] = key.codec.encode(value);

                    return true;
                }
            }
        }

        return false;
    }

    private void ensureCapacity() {
        if (keyAndValue == null) {
            keyAndValue = new byte[INIT_CAPACITY * 2][];
        } else if (size == keyAndValue.length) {
            byte[][] newKeyAndValue = new byte[size * 2][];

            System.arraycopy(keyAndValue, 0, newKeyAndValue, 0, size);

            this.keyAndValue = newKeyAndValue;
        }
    }

    @Override
    public String toString() {
        return MessageMetaData.class.getSimpleName() + "[size=" + size() + ']';
    }
}
