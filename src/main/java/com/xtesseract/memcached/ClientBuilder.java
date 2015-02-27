package com.xtesseract.memcached;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Утилитарный класс для конструирования {@link com.xtesseract.memcached.Client}
 */
public class ClientBuilder {

    public enum ReadStrategy {
        SHARD, RETRY_ON_FAIL
    }

    private static final EventLoopGroup EVENT_LOOP_GROUP = new NioEventLoopGroup();

    private int timeout = 50;

    private EventLoopGroup eventLoopGroup = EVENT_LOOP_GROUP;

    private List<List<InetSocketAddress>> readWriteMirrors = new ArrayList<>();
    private List<List<InetSocketAddress>> writeOnlyMirrors = new ArrayList<>();

    private ReadStrategy readStrategy = ReadStrategy.SHARD;
    private int retryOnFailNumber = -1;
    private int retryOnFailTimeout;

    /**
     * Добавляет зеркало которое будет использоваться для чтения и записи. Данные будут шардироваться по серверам зеркала.
     *
     * @param servers список серверов
     * @return
     */
    public ClientBuilder addReadWriteMirror(List<InetSocketAddress> servers) {
        readWriteMirrors.add(servers);
        return this;
    }

    /**
     * Добавляет зеркало которое будет использоваться только для записи. Данные будут шардироваться по серверам зеркала.
     *
     * @param servers список серверов
     * @return
     */
    public ClientBuilder addWriteOnlyMirror(List<InetSocketAddress> servers) {
        writeOnlyMirrors.add(servers);
        return this;
    }

    public Client build() {
        return new UdpClient(timeout,
                eventLoopGroup,
                readStrategy(),
                shardStrategy(readWriteMirrors),
                shardStrategy(writeOnlyMirrors),
                shardStrategy(concat(readWriteMirrors, writeOnlyMirrors)));
    }

    public ClientBuilder eventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
        return this;
    }

    /**
     * Устанавливает максимальное время ожидания ответа от memcached
     *
     * @param timeout время ожидания в мс.
     * @return
     */
    public ClientBuilder setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public ClientBuilder retryOnFail(int number, int timeout) {
        assert number > 0;
        assert timeout > 0;

        this.readStrategy = ReadStrategy.RETRY_ON_FAIL;
        this.retryOnFailNumber = number;
        this.retryOnFailTimeout = timeout;
        return this;
    }

    private InetSocketAddress[][] asArrays(List<List<InetSocketAddress>> mirrors) {
        InetSocketAddress[][] result = new InetSocketAddress[mirrors.size()][];
        for (int i = 0; i < result.length; ++i) {
            List<InetSocketAddress> mirror = mirrors.get(i);
            result[i] = mirror.toArray(new InetSocketAddress[mirror.size()]);
        }
        return result;
    }

    private List<List<InetSocketAddress>> concat(List<List<InetSocketAddress>> first, List<List<InetSocketAddress>> second) {
        List<List<InetSocketAddress>> result = new ArrayList<>(first.size() + second.size());
        result.addAll(first);
        result.addAll(second);
        return result;
    }

    private int hash(String key) {
        return key.hashCode();
    }

    private ServerStrategy readStrategy() {
        if (ReadStrategy.SHARD.equals(this.readStrategy)) {
            return shardStrategy(readWriteMirrors);
        }
        return retryOnFailStrategy(readWriteMirrors);
    }

    private ServerStrategy retryOnFailStrategy(List<List<InetSocketAddress>> mirrors) {
        InetSocketAddress[][] servers = asArrays(mirrors);
        int numberOfMirrors = servers.length;
        return (promise, channel, key, buf) -> {
            int hash = hash(key);
            ReferenceCountUtil.retain(buf, retryOnFailNumber);

            // Send first main packet
            InetSocketAddress[] mirror = servers[0];
            channel.writeAndFlush(new DatagramPacket(buf, mirror[hash % mirror.length]));

            AtomicReference<ScheduledFuture<?>> retryTask = new AtomicReference<>();
            AtomicInteger retryNumber = new AtomicInteger(0);

            Runnable callback = new Runnable() {
                public void run() {
                    int numberOfRetry = retryNumber.incrementAndGet();
                    int left = retryOnFailNumber - numberOfRetry;
                    if (left < 0) {
                        return;
                    }
                    if (promise.isDone()) {
                        if (left > 0) {
                            ReferenceCountUtil.release(buf, left);
                        }
                        return;
                    }

                    InetSocketAddress[] mirror0 = servers[numberOfRetry % numberOfMirrors];
                    channel.writeAndFlush(new DatagramPacket(buf, mirror0[hash % mirror0.length]));

                    if (left > 0) {
                        retryTask.set(channel.eventLoop().schedule(this, retryOnFailTimeout, TimeUnit.MILLISECONDS));
                    } else {
                        retryTask.set(null);
                    }
                }
            };
            retryTask.set(channel.eventLoop().schedule(callback, retryOnFailTimeout, TimeUnit.MILLISECONDS));
            promise.addListener((Future<Object> future) -> {
                int left = retryOnFailNumber - retryNumber.getAndSet(Integer.MAX_VALUE);
                if (left > 0) {
                    ReferenceCountUtil.release(buf, left);
                }
                ScheduledFuture<?> task = retryTask.get();
                if (null != task) {
                    task.cancel(false);
                }
            });
        };
    }

    private ServerStrategy shardStrategy(List<List<InetSocketAddress>> listOfMirrors) {
        InetSocketAddress[][] servers = asArrays(listOfMirrors);
        int numberOfMirrors = servers.length;
        if (0 == numberOfMirrors) {
            return null;
        }

        int retainAmount = numberOfMirrors - 1;
        return (promise, channel, key, buf) -> {
            int hash = hash(key);
            if (retainAmount > 0) {
                ReferenceCountUtil.retain(buf, retainAmount);
            }
            for (int i = 0; i < numberOfMirrors; ++i) {
                InetSocketAddress[] mirror = servers[i];
                channel.writeAndFlush(new DatagramPacket(buf, mirror[hash % mirror.length]));
            }
        };
    }
}
