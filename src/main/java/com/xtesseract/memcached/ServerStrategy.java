package com.xtesseract.memcached;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.function.BiConsumer;

/**
* Created by tesseract on 29.12.14.
*/
interface ServerStrategy  {
    void accept(Channel channel, String key, ByteBuf buf);
}
