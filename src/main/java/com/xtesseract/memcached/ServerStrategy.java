package com.xtesseract.memcached;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;

import java.util.function.BiConsumer;

/**
* Стратегия отправки запроса к memcached
*/
interface ServerStrategy  {
    void accept(Promise<?> promise, Channel channel, String key, ByteBuf buf);
}
