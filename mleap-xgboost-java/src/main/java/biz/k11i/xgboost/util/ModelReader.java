package biz.k11i.xgboost.util;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

/**
 * Reads the Xgboost model from stream.
 */
public class ModelReader implements Closeable {
    private final InputStream stream;
    private byte[] buffer;

    @Deprecated
    public ModelReader(String filename) throws IOException {
        this(new FileInputStream(filename));
    }

    public ModelReader(InputStream in) throws IOException {
        stream = in;
    }

    private int fillBuffer(int numBytes) throws IOException {
        if (buffer == null || buffer.length < numBytes) {
            buffer = new byte[numBytes];
        }

        int numBytesRead = 0;
        while (numBytesRead < numBytes) {
            int count = stream.read(buffer, numBytesRead, numBytes - numBytesRead);
            if (count < 0) {
                return numBytesRead;
            }
            numBytesRead += count;
        }

        return numBytesRead;
    }

    public int readByteAsInt() throws IOException {
        return stream.read();
    }

    public byte[] readByteArray(int numBytes) throws IOException {
        int numBytesRead = fillBuffer(numBytes);
        if (numBytesRead < numBytes) {
            throw new EOFException(
                    String.format("Cannot read byte array (shortage): expected = %d, actual = %d",
                            numBytes, numBytesRead));
        }

        byte[] result = new byte[numBytes];
        System.arraycopy(buffer, 0, result, 0, numBytes);

        return result;
    }

    public int readInt() throws IOException {
        return readInt(ByteOrder.LITTLE_ENDIAN);
    }

    public int readIntBE() throws IOException {
        return readInt(ByteOrder.BIG_ENDIAN);
    }

    private int readInt(ByteOrder byteOrder) throws IOException {
        int numBytesRead = fillBuffer(4);
        if (numBytesRead < 4) {
            throw new EOFException("Cannot read int value (shortage): " + numBytesRead);
        }

        return ByteBuffer.wrap(buffer).order(byteOrder).getInt();
    }

    public int[] readIntArray(int numValues) throws IOException {
        int numBytesRead = fillBuffer(numValues * 4);
        if (numBytesRead < numValues * 4) {
            throw new EOFException(
                    String.format("Cannot read int array (shortage): expected = %d, actual = %d",
                            numValues * 4, numBytesRead));
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);

        int[] result = new int[numValues];
        for (int i = 0; i < numValues; i++) {
            result[i] = byteBuffer.getInt();
        }

        return result;
    }

    public int readUnsignedInt() throws IOException {
        int result = readInt();
        if (result < 0) {
            throw new IOException("Cannot read unsigned int (overflow): " + result);
        }

        return result;
    }

    public long readLong() throws IOException {
        int numBytesRead = fillBuffer(8);
        if (numBytesRead < 8) {
            throw new IOException("Cannot read long value (shortage): " + numBytesRead);
        }

        return ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    public float asFloat(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    public int asUnsignedInt(byte[] bytes) throws IOException {
        int result = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (result < 0) {
            throw new IOException("Cannot treat as unsigned int (overflow): " + result);
        }

        return result;
    }

    public float readFloat() throws IOException {
        int numBytesRead = fillBuffer(4);
        if (numBytesRead < 4) {
            throw new IOException("Cannot read float value (shortage): " + numBytesRead);
        }

        return ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    public float[] readFloatArray(int numValues) throws IOException {
        int numBytesRead = fillBuffer(numValues * 4);
        if (numBytesRead < numValues * 4) {
            throw new EOFException(
                    String.format("Cannot read float array (shortage): expected = %d, actual = %d",
                            numValues * 4, numBytesRead));
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);

        float[] result = new float[numValues];
        for (int i = 0; i < numValues; i++) {
            result[i] = byteBuffer.getFloat();
        }

        return result;
    }

    public double[] readDoubleArrayBE(int numValues) throws IOException {
        int numBytesRead = fillBuffer(numValues * 8);
        if (numBytesRead < numValues * 8) {
            throw new EOFException(
                    String.format("Cannot read double array (shortage): expected = %d, actual = %d",
                            numValues * 8, numBytesRead));
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);

        double[] result = new double[numValues];
        for (int i = 0; i < numValues; i++) {
            result[i] = byteBuffer.getDouble();
        }

        return result;
    }

    public void skip(long numBytes) throws IOException {
        long numBytesRead = stream.skip(numBytes);
        if (numBytesRead < numBytes) {
            throw new IOException("Cannot skip bytes: " + numBytesRead);
        }
    }

    public String readString() throws IOException {
        long length = readLong();
        if (length > Integer.MAX_VALUE) {
            throw new IOException("Too long string: " + length);
        }

        return readString((int) length);
    }

    public String readString(int numBytes) throws IOException {
        int numBytesRead = fillBuffer(numBytes);
        if (numBytesRead < numBytes) {
            throw new IOException(String.format("Cannot read string(%d) (shortage): %d", numBytes, numBytesRead));
        }

        return new String(buffer, 0, numBytes, Charset.forName("UTF-8"));
    }

    public String readUTF() throws IOException {
        int utflen = readByteAsInt();
        utflen = (short)((utflen << 8) | readByteAsInt());
        return readUTF(utflen);
    }

    public String readUTF(int utflen) throws IOException {
        int numBytesRead = fillBuffer(utflen);
        if (numBytesRead < utflen) {
            throw new EOFException(
                    String.format("Cannot read UTF string bytes: expected = %d, actual = %d",
                            utflen, numBytesRead));
        }

        char[] chararr = new char[utflen];

        int c, char2, char3;
        int count = 0;
        int chararr_count=0;

        while (count < utflen) {
            c = (int) buffer[count] & 0xff;
            if (c > 127) break;
            count++;
            chararr[chararr_count++]=(char)c;
        }

        while (count < utflen) {
            c = (int) buffer[count] & 0xff;
            switch (c >> 4) {
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    /* 0xxxxxxx*/
                    count++;
                    chararr[chararr_count++]=(char)c;
                    break;
                case 12: case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    char2 = (int) buffer[count-1];
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException(
                                "malformed input around byte " + count);
                    chararr[chararr_count++]=(char)(((c & 0x1F) << 6) |
                            (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    char2 = (int) buffer[count-2];
                    char3 = (int) buffer[count-1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (count-1));
                    chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
                            ((char2 & 0x3F) << 6)  |
                            ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException(
                            "malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
