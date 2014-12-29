package com.xtesseract.memcached;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.xtesseract.memcached.ProtocolHelper.*;


/**
 * Created by tesseract on 15.12.14.
 */
public class UdpClient implements Client {

    private static class PacketInboundHandler extends SimpleChannelInboundHandler<DatagramPacket> {
        private final ConcurrentHashMap<Integer, Promise<?>> callbacks;

        public PacketInboundHandler(ConcurrentHashMap<Integer, Promise<?>> callbacks) {
            this.callbacks = callbacks;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
            ByteBuf buf = msg.content();

            // UDP header
            int requestId = buf.readUnsignedShort();
            int sequenceNumber = buf.readShort();
            int totalNumberOfDatagrams = buf.readShort();
            int reserved = buf.readShort();

            Promise promise = callbacks.remove(requestId);
            if (null == promise) {
                // Ответ уже получен или истек таймаут
                return;
            }

            // Operation header
            short magic = buf.readUnsignedByte(); // Magic number.
            if (RESPONSE_PACKET_MAGIC != magic) {
                // Пакет не от memcahed
                return;
            }
            byte opCode = buf.readByte();        // Command code
            int keyLength = buf.readShort();     // Length in bytes of the text key that follows the command extras
            byte extrasLength = buf.readByte();  // Length in bytes of the command extras
            byte dataType = buf.readByte();      // Reserved for future use (Sean is using this soon).
            int status = buf.readShort();        // Status of the response (non-zero on error).
            if (0 != status) {
                promise.tryFailure(new OperationError(status));
                return;
            }
            int totalBodyLength = buf.readInt(); // Length in bytes of extra + key + value
            int opaque = buf.readInt();          // Will be copied back to you in the response.
            long cas = buf.readLong();           // Data version check

            int bodyLength = totalBodyLength - extrasLength - keyLength;

            // Data
            ByteBuf extras = buf.readBytes(extrasLength);
            ByteBuf key = buf.readBytes(keyLength);
            ByteBuf body = buf.readBytes(bodyLength);

            switch (opCode) {
                case Operation.GET:
                    promise.trySuccess(body.toString(CharsetUtil.UTF_8));
                    return;

                case Operation.ADD:
                case Operation.REPLACE:
                case Operation.SET:
                    promise.trySuccess(null);
                    return;

                case Operation.DECREMENT:
                case Operation.INCREMENT:
                    promise.trySuccess(body.readLong());
                    return;

                default:
                    throw new IllegalArgumentException("Unsupported operation: " + opCode);
            }
        }
    }

    private final ServerStrategy readStrategy;
    private final ServerStrategy writeStrategy;

    private final int retryTimeout; // ms
    private final int timeout; // ms.

    private final Channel channel;
    private final ConcurrentHashMap<Integer, Promise<?>> callbacks;

    private final AtomicInteger requestIdCounter = new AtomicInteger((int) (System.currentTimeMillis() * 100));

    UdpClient(int retryTimeout,
              int timeout,
              EventLoopGroup group,
              ServerStrategy readStrategy,
              ServerStrategy writeStrategy) {
        this.callbacks = new ConcurrentHashMap<>();

        this.readStrategy = readStrategy;
        this.writeStrategy = writeStrategy;

        this.retryTimeout = retryTimeout;
        this.timeout = timeout;

        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioDatagramChannel.class)
                .handler(new PacketInboundHandler(callbacks));

        try {
            channel = b.bind(0).sync().channel();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addQ(String key, int exp, String value) {
        executeSetQ(Operation.ADD_Q, key, exp, value);
    }

    @Override
    public Promise<Long> dec(String key, int exp) {
        return dec(key, exp, 1, 0);
    }

    @Override
    public Promise<Long> dec(String key, int exp, long incValue, long initialValue) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, () -> sendIncOrDecOperation(requestId, Operation.DECREMENT, key, exp, incValue, initialValue));
    }

    @Override
    public void decQ(String key, int exp, long incValue, long initialValue) {
        sendIncOrDecOperation(nextRequestId(), Operation.DECREMENT_Q, key, exp, incValue, initialValue);
    }

    @Override
    public Promise<Void> delete(String key) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, () -> sendWriteSimpleKeyPacketOperation(requestId, Operation.DELETE, key));
    }

    @Override
    public void deleteQ(String key) {
        sendWriteSimpleKeyPacketOperation(nextRequestId(), Operation.DELETE_Q, key);
    }

    @Override
    public Promise<String> get(String key) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, () -> sendReadSimpleKeyPacketOperation(requestId, Operation.GET, key));
    }

    @Override
    public Promise<Long> inc(String key, int exp) {
        return inc(key, exp, 1, 0);
    }

    @Override
    public Promise<Long> inc(String key, int exp, long incValue, long initialValue) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, () -> sendIncOrDecOperation(requestId, Operation.INCREMENT, key, exp, incValue, initialValue));
    }

    @Override
    public void incQ(String key, int exp, long incValue, long initialValue) {
        sendIncOrDecOperation(nextRequestId(), Operation.INCREMENT_Q, key, exp, incValue, initialValue);
    }

    @Override
    public void replaceQ(String key, int exp, String value) {
        executeSetQ(Operation.REPLACE_Q, key, exp, value);
    }

    @Override
    public Promise<Void> set(String key, int exp, String value) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, () -> sendSetOperation(requestId, Operation.SET, key, exp, value));
    }

    @Override
    public void setQ(String key, int exp, String value) {
        executeSetQ(Operation.SET_Q, key, exp, value);
    }

    private void executeSetQ(byte opCode, String key, int exp, String value) {
        sendSetOperation(nextRequestId(), opCode, key, exp, value);
    }

    private int nextRequestId() {
        return requestIdCounter.incrementAndGet() & 0xffff;
    }

    private <V> Promise<V> sendAndWaitResult(Integer requestId, Runnable method) {
        Promise<V> promise = channel.eventLoop().newPromise();
        callbacks.put(requestId, promise);

        // ??????? потенциальная утечка в callbacks
        ScheduledFuture<?> schedule = channel.eventLoop().schedule(() -> {
            Promise<?> p = callbacks.get(requestId);
            if (null != p && !p.isDone()) {
                // resend packet
                method.run();

                ScheduledFuture<?> schedule1 = channel.eventLoop().schedule(() -> {
                    Promise<?> pr = callbacks.get(requestId);
                    if (null != pr && !pr.isDone()) {
                        pr.tryFailure(new TimeoutException());
                        callbacks.remove(requestId);
                    }
                }, timeout, TimeUnit.MILLISECONDS);

                promise.addListener(f -> schedule1.cancel(false));
            }
        }, retryTimeout, TimeUnit.MILLISECONDS);

        promise.addListener(f -> schedule.cancel(false));

        method.run();

        return promise;
    }

    private void sendWriteSimpleKeyPacketOperation(int requestId, byte opCode, String key) {
        writeStrategy.accept(channel, key, getSimpleKeyPacket(channel, requestId, opCode, key));
    }

    private void sendReadSimpleKeyPacketOperation(int requestId, byte opCode, String key) {
        readStrategy.accept(channel, key, getSimpleKeyPacket(channel, requestId, opCode, key));
    }

    private void sendSetOperation(int requestId, byte opCode, String key, int exp, String value) {
        writeStrategy.accept(channel, key, getSetPacket(channel, requestId, opCode, key, exp, value));
    }

    private void sendIncOrDecOperation(int requestId, byte opCode, String key, int exp, long incValue, long initialValue) {
        writeStrategy.accept(channel, key, getIncPacket(channel, requestId, opCode, key, exp, incValue, initialValue));
    }
}
