package com.xtesseract.memcached;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.CharsetUtil;

/**
 * Created by tesseract on 15.12.14.
 */
public class ProtocolHelper {

    public static final short REQUEST_PACKET_MAGIC = 0x80;
    public static final short RESPONSE_PACKET_MAGIC = 0x81;

    public static final int UDP_HEADER_LENGTH = 8;
    public static final int COMMAND_HEADER_LENGTH = 24;


    public static final int SET_EXTRA_LENGTH = 8;
    public static final int INC_EXTRA_LENGTH = 20;

    public static ByteBuf getIncPacket(Channel channel, int requestId, byte opCode, String key, int exp, long amountToAdd, long initialValue) {
        byte[] keyBytes = key.getBytes(CharsetUtil.UTF_8);

        int packetSize = UDP_HEADER_LENGTH +
                COMMAND_HEADER_LENGTH +
                INC_EXTRA_LENGTH +
                keyBytes.length;

        int totalDataLength = INC_EXTRA_LENGTH + keyBytes.length;

        ByteBuf buf = channel.alloc().buffer(packetSize, packetSize);
        writeUdpHeader(buf, requestId, 0, 1);
        writePacketHeader(buf, requestId, opCode, totalDataLength, keyBytes.length, INC_EXTRA_LENGTH);

        // Extras
        buf.writeLong(amountToAdd);
        buf.writeLong(initialValue);
        buf.writeInt(exp); // expiration time

        // Body
        buf.writeBytes(keyBytes);

        return buf;
    }

    public static ByteBuf getSetPacket(Channel channel, int requestId, byte opCode, String key, int exp, String value) {
        byte[] keyBytes = key.getBytes(CharsetUtil.UTF_8);
        byte[] valueBytes = value.getBytes(CharsetUtil.UTF_8);

        int packetSize = UDP_HEADER_LENGTH +
                COMMAND_HEADER_LENGTH +
                SET_EXTRA_LENGTH +
                keyBytes.length +
                valueBytes.length;

        int totalDataLength = SET_EXTRA_LENGTH + keyBytes.length + valueBytes.length;

        ByteBuf buf = channel.alloc().buffer(packetSize, packetSize);
        writeUdpHeader(buf, requestId, 0, 1);
        writePacketHeader(buf, requestId, opCode, totalDataLength, keyBytes.length, SET_EXTRA_LENGTH);

        // Extras
        buf.writeInt(0); // flags
        buf.writeInt(exp); // expiration time

        // Body
        buf.writeBytes(keyBytes);
        buf.writeBytes(valueBytes);

        return buf;
    }

    public static ByteBuf getSimpleKeyPacket(Channel channel, int requestId, byte opCode, String key) {
        byte[] keyBytes = key.getBytes(CharsetUtil.UTF_8);

        int packetSize = UDP_HEADER_LENGTH +
                COMMAND_HEADER_LENGTH +
                keyBytes.length;

        int totalDataLength = keyBytes.length;

        ByteBuf buf = channel.alloc().buffer(packetSize, packetSize);
        writeUdpHeader(buf, requestId, 0, 1);
        writePacketHeader(buf, requestId, opCode, totalDataLength, keyBytes.length, 0);

        // Extras

        // Body
        buf.writeBytes(keyBytes);

        return buf;
    }

    /**
     * <pre>
     *  Byte/     0       |       1       |       2       |       3       |
     *     /              |               |               |               |
     *    |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
     *    +---------------+---------------+---------------+---------------+
     *   0| Magic         | Opcode        | Key length                    |
     *    +---------------+---------------+---------------+---------------+
     *   4| Extras length | Data type     | Reserved                      |
     *    +---------------+---------------+---------------+---------------+
     *   8| Total body length                                             |
     *    +---------------+---------------+---------------+---------------+
     *  12| Opaque                                                        |
     *    +---------------+---------------+---------------+---------------+
     *  16| CAS                                                           |
     *    |                                                               |
     *    +---------------+---------------+---------------+---------------+
     * Total 24 bytes
     * </pre>
     *
     * @param buf
     * @param opCode
     * @param totalDataLength
     * @param keyLength
     * @param extraLength
     */
    public static void writePacketHeader(ByteBuf buf,
                                         int requestId,
                                         byte opCode,
                                         int totalDataLength,
                                         int keyLength,
                                         int extraLength) {
        buf.writeByte(REQUEST_PACKET_MAGIC); // Magic number
        buf.writeByte(opCode); // Command code
        buf.writeShort(keyLength);
        buf.writeByte(extraLength); // Extras length
        buf.writeByte(0x00); // Data type
        buf.writeShort(0); // Reserved
        buf.writeInt(totalDataLength); // Length in bytes of extra + key + value
        buf.writeInt(requestId); // Will be copied back to you in the response
        buf.writeLong(0); // Data version check.
    }

    /**
     * UDP protocol
     * ------------
     * <p/>
     * For very large installations where the number of clients is high enough
     * that the number of TCP connections causes scaling difficulties, there is
     * also a UDP-based interface. The UDP interface does not provide guaranteed
     * delivery, so should only be used for operations that aren't required to
     * succeed; typically it is used for "get" requests where a missing or
     * incomplete response can simply be treated as a cache miss.
     * <p/>
     * Each UDP datagram contains a simple frame header, followed by data in the
     * same format as the TCP protocol described above. In the current
     * implementation, requests must be contained in a single UDP datagram, but
     * responses may span several datagrams. (The only common requests that would
     * span multiple datagrams are huge multi-key "get" requests and "set"
     * requests, both of which are more suitable to TCP transport for reliability
     * reasons anyway.)
     * <p/>
     * The frame header is 8 bytes long, as follows (all values are 16-bit integers
     * in network byte order, high byte first):
     * <p/>
     * 0-1 Request ID
     * 2-3 Sequence number
     * 4-5 Total number of datagrams in this message
     * 6-7 Reserved for future use; must be 0
     * <p/>
     * The request ID is supplied by the client. Typically it will be a
     * monotonically increasing value starting from a random seed, but the client
     * is free to use whatever request IDs it likes. The server's response will
     * contain the same ID as the incoming request. The client uses the request ID
     * to differentiate between responses to outstanding requests if there are
     * several pending from the same server; any datagrams with an unknown request
     * ID are probably delayed responses to an earlier request and should be
     * discarded.
     * <p/>
     * The sequence number ranges from 0 to n-1, where n is the total number of
     * datagrams in the message. The client should concatenate the payloads of the
     * datagrams for a given response in sequence number order; the resulting byte
     * stream will contain a complete response in the same format as the TCP
     * protocol (including terminating \r\n sequences).
     *
     * @param buf
     * @param requestId
     * @param sequenceNumber
     * @param totalNumber
     */
    public static void writeUdpHeader(ByteBuf buf, int requestId, int sequenceNumber, int totalNumber) {
        buf.writeShort(requestId & 0xffff);
        buf.writeShort(sequenceNumber); // Sequence number
        buf.writeShort(totalNumber); // Total number of datagrams in this message
        buf.writeShort(0); // Reserved for future use; must be 0
    }
}
