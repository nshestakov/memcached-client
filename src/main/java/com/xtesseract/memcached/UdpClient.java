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
import java.util.function.Consumer;

import static com.xtesseract.memcached.ProtocolHelper.*;


/**
 * Реализация  {@link com.xtesseract.memcached.Client memcahed клиента} бинароного протокола использующего
 * в качестве транспорта UDP
 */
public class UdpClient implements Client {

    private interface PacketFactory {
        ByteBuf apply(byte opCode);
    }

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
            int totalBodyLength = buf.readInt(); // Length in bytes of extra + key + value
            int opaque = buf.readInt();          // Will be copied back to you in the response.
            long cas = buf.readLong();           // Data version check

            Promise promise = callbacks.remove(opaque);
            if (null == promise) {
                // Ответ уже получен или истек таймаут
                return;
            }
            if (0 != status) {
                promise.tryFailure(new OperationError(status));
                return;
            }

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
                    promise.tryFailure(new IllegalArgumentException("Unsupported operation: " + opCode));
            }
        }
    }

    private final ServerStrategy readStrategy;
    private final ServerStrategy readOnWriteStrategy;
    private final ServerStrategy writeOnlyStrategy;
    private final ServerStrategy commonStrategy;

    private final int timeout; // ms.

    private final Channel channel;
    private final ConcurrentHashMap<Integer, Promise<?>> callbacks;

    private final AtomicInteger requestIdCounter = new AtomicInteger((int) (System.currentTimeMillis() * 100));

    UdpClient(int timeout,
              EventLoopGroup group,
              ServerStrategy readStrategy,
              ServerStrategy readOnWriteStrategy,
              ServerStrategy writeOnlyStrategy,
              ServerStrategy commonStrategy) {
        this.callbacks = new ConcurrentHashMap<>();

        this.readStrategy = readStrategy;
        this.readOnWriteStrategy = readOnWriteStrategy;
        this.writeOnlyStrategy = writeOnlyStrategy;
        this.commonStrategy = commonStrategy;

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

    public int pendingRequests() {
        return callbacks.size();
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
        return sendAndWaitResult(requestId, (promise) -> sendIncOrDecOperation(requestId, Operation.DECREMENT, Operation.DECREMENT_Q, key, exp, incValue, initialValue));
    }

    @Override
    public void decQ(String key, int exp, long incValue, long initialValue) {
        sendIncOrDecOperation(nextRequestId(), Operation.DECREMENT_Q, Operation.DECREMENT_Q, key, exp, incValue, initialValue);
    }

    @Override
    public Promise<Void> delete(String key) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, (promise) -> sendWriteSimpleKeyPacketOperation(requestId, Operation.DELETE, Operation.DELETE_Q, key));
    }

    @Override
    public void deleteQ(String key) {
        sendWriteSimpleKeyPacketOperation(nextRequestId(), Operation.DELETE_Q, Operation.DELETE_Q, key);
    }

    @Override
    public Promise<String> get(String key) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, (promise) -> sendReadSimpleKeyPacketOperation(promise, requestId, Operation.GET, key));
    }

    @Override
    public Promise<Long> inc(String key, int exp) {
        return inc(key, exp, 1, 1);
    }

    @Override
    public Promise<Long> inc(String key, int exp, long incValue, long initialValue) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, (promise) -> sendIncOrDecOperation(requestId, Operation.INCREMENT, Operation.INCREMENT_Q, key, exp, incValue, initialValue));
    }

    @Override
    public void incQ(String key, int exp, long incValue, long initialValue) {
        sendIncOrDecOperation(nextRequestId(), Operation.INCREMENT_Q, Operation.INCREMENT_Q, key, exp, incValue, initialValue);
    }

    @Override
    public void replaceQ(String key, int exp, String value) {
        executeSetQ(Operation.REPLACE_Q, key, exp, value);
    }

    @Override
    public Promise<Void> set(String key, int exp, String value) {
        int requestId = nextRequestId();
        return sendAndWaitResult(requestId, (promise) -> sendSetOperation(requestId, Operation.SET, Operation.SET_Q, key, exp, value));
    }

    @Override
    public void setQ(String key, int exp, String value) {
        executeSetQ(Operation.SET_Q, key, exp, value);
    }

    private void executeSetQ(byte opCode, String key, int exp, String value) {
        sendSetOperation(nextRequestId(), opCode, opCode, key, exp, value);
    }

    private int nextRequestId() {
        return requestIdCounter.incrementAndGet();
    }

    private <V> Promise<V> sendAndWaitResult(Integer requestId, Consumer<Promise<?>> method) {
        Promise<V> promise = channel.eventLoop().newPromise();
        callbacks.put(requestId, promise);

        ScheduledFuture<?> scheduleTimeout = channel.eventLoop().schedule(() -> {
            callbacks.remove(requestId);
            promise.tryFailure(new TimeoutException());
        }, timeout, TimeUnit.MILLISECONDS);

        promise.addListener(f -> scheduleTimeout.cancel(false));

        method.accept(promise);

        return promise;
    }

    private void sendIncOrDecOperation(int requestId, byte readOpCode, byte writeOpCode, String key, int exp, long incValue, long initialValue) {
        send(readOpCode, writeOpCode, key, (opCode) -> getIncPacket(channel, requestId, opCode, key, exp, incValue, initialValue));
    }

    private void sendReadSimpleKeyPacketOperation(Promise<?> promise, int requestId, byte readOpCode, String key) {
        send(promise, readOpCode, key, readStrategy, (opCode) -> getSimpleKeyPacket(channel, requestId, opCode, key));
    }

    private void sendSetOperation(int requestId, byte readOpCode, byte writeOpCode, String key, int exp, String value) {
        send(readOpCode, writeOpCode, key, (opCode) -> getSetPacket(channel, requestId, opCode, key, exp, value));
    }

    private void send(byte readOpCode, byte writeOpCode, String key, PacketFactory packetCreator) {
        if (readOpCode == writeOpCode) {
            send(null, readOpCode, key, commonStrategy, packetCreator);
        } else {
            send(null, readOpCode, key, readOnWriteStrategy, packetCreator);
            send(null, writeOpCode, key, writeOnlyStrategy, packetCreator);
        }
    }

    private void send(Promise<?> promise, byte opCode, String key, ServerStrategy strategy, PacketFactory packetCreator) {
        if (null != strategy) {
            strategy.accept(promise, channel, key, packetCreator.apply(opCode));
        }
    }

    private void sendWriteSimpleKeyPacketOperation(int requestId, byte readOpCode, byte writeOpCode, String key) {
        send(readOpCode, writeOpCode, key, (opCode) -> getSimpleKeyPacket(channel, requestId, opCode, key));
    }
}
