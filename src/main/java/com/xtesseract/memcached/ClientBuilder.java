package com.xtesseract.memcached;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Утилитарный класс для конструирования {@link com.xtesseract.memcached.Client}
 */
public class ClientBuilder {

    private static final EventLoopGroup EVENT_LOOP_GROUP = new NioEventLoopGroup();

    private int timeout = 50;

    private EventLoopGroup eventLoopGroup = EVENT_LOOP_GROUP;

    private List<List<InetSocketAddress>> readWriteMirrors = new ArrayList<>();
    private List<List<InetSocketAddress>> writeOnlyMirrors = new ArrayList<>();

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

    private ServerStrategy shardStrategy(List<List<InetSocketAddress>> listOfMirrors) {
        InetSocketAddress[][] servers = asArrays(listOfMirrors);
        int numberOfMirrors = servers.length;
        if (0 == numberOfMirrors) {
            return null;
        }

        int retainAmount = numberOfMirrors - 1;
        return (chanel, key, buf) -> {
            int hash = hash(key);
            if (retainAmount > 0) {
                ReferenceCountUtil.retain(buf, retainAmount);
            }
            for (int i = 0; i < numberOfMirrors; ++i) {
                InetSocketAddress[] mirror = servers[i];
                chanel.writeAndFlush(new DatagramPacket(buf, mirror[hash % mirror.length]));
            }
        };
    }
}
