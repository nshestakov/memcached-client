package com.xtesseract.memcached;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tesseract on 29.12.14.
 */
public class ClientBuilder {


    private static final EventLoopGroup EVENT_LOOP_GROUP = new NioEventLoopGroup();

    private int timeout = 50;
    private int retryTimeout = 25;

    private EventLoopGroup eventLoopGroup = EVENT_LOOP_GROUP;

    private List<List<InetSocketAddress>> servers = new ArrayList<>();

    public Client build() {
        InetSocketAddress[] allServers = allServers();
        int serversInMirror = allServers.length / servers.size();

        boolean isSimple = 1 == serversInMirror;
        return new UdpClient(retryTimeout,
                timeout,
                eventLoopGroup,
                isSimple ? simpleReadStrategy(allServers) : shardReadStrategy(allServers),
                isSimple ? simpleWriteStrategy(allServers) : shardWriteStrategy(allServers));
    }

    public ClientBuilder eventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
        return this;
    }

    public ClientBuilder retryTimeout(int retryTimeout) {
        this.retryTimeout = retryTimeout;
        return this;
    }

    public ClientBuilder setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public ClientBuilder addMirror(List<InetSocketAddress> mirrorServers) {
        servers.add(mirrorServers);
        return this;
    }

    private InetSocketAddress[] allServers() {
        return servers.stream().flatMap(v -> v.stream()).toArray(InetSocketAddress[]::new);
    }

    private ServerStrategy simpleReadStrategy(InetSocketAddress[] servers) {
        InetSocketAddress server = servers[0];
        return (chanel, key, buf) -> {
            chanel.writeAndFlush(new DatagramPacket(buf, server));
        };
    }

    private ServerStrategy simpleWriteStrategy(InetSocketAddress[] servers) {
        int numberOfMirrors = this.servers.size();
        return (chanel, key, buf) -> {
            if (numberOfMirrors > 1) {
                ReferenceCountUtil.retain(buf, numberOfMirrors - 1);
            }
            for (int i = 0; i < servers.length; ++i) {
                chanel.writeAndFlush(new DatagramPacket(buf, servers[i]));
            }
        };
    }

    private ServerStrategy shardReadStrategy(InetSocketAddress[] servers) {
        int numberOfMirrors = this.servers.size();
        int serversInMirror = servers.length / numberOfMirrors;
        return (chanel, key, buf) -> {
            chanel.writeAndFlush(new DatagramPacket(buf, servers[hash(key, serversInMirror)]));
        };
    }

    private ServerStrategy shardWriteStrategy(InetSocketAddress[] servers) {
        int numberOfMirrors = this.servers.size();
        int serversInMirror = servers.length / numberOfMirrors;
        return (chanel, key, buf) -> {
            for (int i = hash(key, serversInMirror); i < servers.length; i += serversInMirror) {
                chanel.writeAndFlush(new DatagramPacket(buf, servers[i]));
            }
        };
    }

    private int hash(String key, int serversInMirror) {
        return key.hashCode() % serversInMirror;
    }
}
