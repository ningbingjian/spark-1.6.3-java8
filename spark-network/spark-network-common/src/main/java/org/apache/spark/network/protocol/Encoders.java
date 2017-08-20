package org.apache.spark.network.protocol;

/**
 * Created by zhaoshufen
 * User:  zhaoshufen
 * Date: 2017/8/19
 * Time: 12:14
 * To change this setting on:Preferences->editor->File and Code Templates->Include->File Header
 */

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;

/** Provides a canonical set of Encoders for simple types. */
public class Encoders {
    /** Strings are encoded with their length followed by UTF-8 bytes. */
    public static class Strings {
        public static int encodedLength(String s) {
            return 4 + s.getBytes(Charsets.UTF_8).length;
        }
        public static void encode(ByteBuf buf, String s) {
            byte[] bytes = s.getBytes(Charsets.UTF_8);
            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);
        }
        public static String decode(ByteBuf buf) {
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return new String(bytes, Charsets.UTF_8);
        }
    }
    public static String decode(ByteBuf buf) {
        int length = buf.readInt();
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return new String(bytes, Charsets.UTF_8);
    }
    /** Byte arrays are encoded with their length followed by bytes. */
    public static class ByteArrays {
        public static int encodedLength(byte[] arr) {
            return 4 + arr.length;
        }
        public static void encode(ByteBuf buf, byte[] arr) {
            buf.writeInt(arr.length);
            buf.writeBytes(arr);
        }

        public static byte[] decode(ByteBuf buf) {
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return bytes;
        }
    }
    /** String arrays are encoded with the number of strings followed by per-String encoding. */
    public static class StringArrays {
        public static int encodedLength(String[] strings) {
            int totalLength = 4;
            for (String s : strings) {
                totalLength += Strings.encodedLength(s);
            }
            return totalLength;
        }
        public static void encode(ByteBuf buf, String[] strings) {
            buf.writeInt(strings.length);
            for (String s : strings) {
                Strings.encode(buf, s);
            }
        }
        public static String[] decode(ByteBuf buf) {
            int numStrings = buf.readInt();
            String[] strings = new String[numStrings];
            for (int i = 0; i < strings.length; i ++) {
                strings[i] = Strings.decode(buf);
            }
            return strings;
        }
    }
}
