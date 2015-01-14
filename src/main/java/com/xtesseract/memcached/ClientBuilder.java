package com.xtesseract.memcached;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by tesseract on 29.12.14.
 */
public class ClientBuilder {

    public static class DC {
        private boolean main = false;
        private List<List<InetSocketAddress>> mirrors = new ArrayList<>();

        public boolean isMain() {
            return main;
        }

        public DC main(boolean main) {
            this.main = main;
            return this;
        }

        public List<InetSocketAddress> newMirror() {
            List<InetSocketAddress> mirror = new ArrayList<>();
            mirrors.add(mirror);
            return mirror;
        }
    }

    private static final EventLoopGroup EVENT_LOOP_GROUP = new NioEventLoopGroup();

    private int timeout = 50;

    private EventLoopGroup eventLoopGroup = EVENT_LOOP_GROUP;

    private List<DC> DCs = new ArrayList<>();

    public DC addDC() {
        DC dc = new DC();
        DCs.add(dc);
        return dc;
    }

    public Client build() {
        Optional<DC> readDCo = DCs.stream().findAny().filter(dc -> dc.isMain());
        DC mainDC = readDCo.orElseGet(() -> DCs.get(0));

        DC[] otherDCs = DCs.stream().filter(dc -> !dc.isMain()).toArray(DC[]::new);

        return new UdpClient(timeout,
                eventLoopGroup,
                shardReadStrategy(mainDC),
                shardWriteStrategy(otherDCs));
    }

    public ClientBuilder eventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
        return this;
    }

    public ClientBuilder setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    private int hash(String key) {
        return key.hashCode();
    }

    private ServerStrategy shardReadStrategy(DC dc) {
        List<List<InetSocketAddress>> mirrors = dc.mirrors;
        int numberOfMirrors = mirrors.size();
        int retainAmount = numberOfMirrors - 1;
        return (chanel, key, buf) -> {
            int hash = hash(key);
            if (retainAmount > 0) {
                ReferenceCountUtil.retain(buf, retainAmount);
            }
            mirrors.forEach(shards -> chanel.writeAndFlush(new DatagramPacket(buf, shards.get(hash % shards.size()))));
        };
    }

    private ServerStrategy shardWriteStrategy(DC[] otherDCs) {
        if (0 == otherDCs.length) {
            return (chanel, key, buf) -> ReferenceCountUtil.release(buf);
        }

        return (chanel, key, buf) -> {
            ReferenceCountUtil.retain(buf);
            int hash = hash(key);
            for (int i = 0; i < otherDCs.length; ++i) {
                DC dc = otherDCs[i];
                int numberOfMirrors = dc.mirrors.size();
                if (numberOfMirrors > 0) {
                    ReferenceCountUtil.retain(buf, numberOfMirrors);
                }
                dc.mirrors.forEach(shards -> chanel.writeAndFlush(new DatagramPacket(buf, shards.get(hash % shards.size()))));
            }
            ReferenceCountUtil.release(buf, otherDCs.length);
        };
    }
}
