package com.xtesseract.memcached;

import io.netty.util.concurrent.Promise;

/**
 * Created by tesseract on 15.12.14.
 */
public interface Client {

    void addQ(String key, int exp, String value);

    Promise<Long> dec(String key, int exp);

    Promise<Long> dec(String key, int exp, long incValue, long initialValue);

    void decQ(String key, int exp, long incValue, long initialValue);

    Promise<Void> delete(String key);

    void deleteQ(String key);

    Promise<String> get(String key);

    Promise<Long> inc(String key, int exp);

    Promise<Long> inc(String key, int exp, long incValue, long initialValue);

    void incQ(String key, int exp, long incValue, long initialValue);

    void replaceQ(String key, int exp, String value);

    Promise<Void> set(String key, int exp, String value);

    void setQ(String key, int exp, String value);
}
