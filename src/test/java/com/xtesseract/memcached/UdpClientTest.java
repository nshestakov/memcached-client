package com.xtesseract.memcached;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Nikolay Shestakov <ns@xtesseract.com>
 */
public class UdpClientTest {

    public static final int DEFAULT_EXP = 60;
    private Client client;

    @Test
    public void dec() throws Exception {
        // настройка системы
        String key = randomString();
        int initialValue = randomInt();
        int amount = randomInt();

        // вызов системы
        long result = client.dec(key, DEFAULT_EXP, amount, initialValue).get(1, TimeUnit.SECONDS);

        // проверка утверждений
        Assert.assertEquals(initialValue, result);
    }

    @Test
    public void decExists() throws Exception {
        // настройка системы
        String key = randomString();
        int initialValue = randomInt();
        int amount = randomInt();
        client.dec(key, DEFAULT_EXP, amount, initialValue + amount).get(1, TimeUnit.SECONDS);

        // вызов системы
        long result = client.dec(key, DEFAULT_EXP, amount, initialValue).get(1, TimeUnit.SECONDS);

        // проверка утверждений
        Assert.assertEquals(initialValue, result);
    }

    @Test
    public void deleteQ() throws Exception {
        // настройка системы
        String key = randomString();
        client.set(key, DEFAULT_EXP, randomString()).get(1, TimeUnit.SECONDS);

        // вызов системы
        client.deleteQ(key);

        // проверка утверждений
        try {
            client.get(key).get(1, TimeUnit.SECONDS);
            Assert.fail("Key isn't exists");
        } catch (Exception e) {
            int status = ((OperationError) e.getCause()).getStatus();
            Assert.assertEquals("Key not exists", 1, status);
        }
    }

    @Test
    public void inc() throws Exception {
        // настройка системы
        String key = randomString();
        int initialValue = randomInt();
        int amount = randomInt();

        // вызов системы
        long result = client.inc(key, DEFAULT_EXP, amount, initialValue).get(1, TimeUnit.SECONDS);

        // проверка утверждений
        Assert.assertEquals(initialValue, result);
    }

    @Test
    public void incAndGet() throws Exception {
        // настройка системы
        String key = randomString();
        int initialValue = randomInt();
        int amount = randomInt();

        client.inc(key, DEFAULT_EXP, amount, initialValue).get(1, TimeUnit.SECONDS);

        // вызов системы
        String result = client.get(key).get(1, TimeUnit.SECONDS);

        // проверка утверждений
        Assert.assertEquals(String.valueOf(initialValue), result);
    }

    @Test
    public void incExists() throws Exception {
        // настройка системы
        String key = randomString();
        int initialValue = randomInt();
        int amount = randomInt();
        client.inc(key, DEFAULT_EXP, amount, initialValue).get(1, TimeUnit.SECONDS);

        // вызов системы
        long result = client.inc(key, DEFAULT_EXP, amount, initialValue).get(1, TimeUnit.SECONDS);

        // проверка утверждений
        Assert.assertEquals(initialValue + amount, result);
    }

    @Test
    public void incFromWrongPort() throws Exception {
        ClientBuilder builder = new ClientBuilder()
                .setTimeout(50);
        builder.addReadWriteMirror(Arrays.asList(new InetSocketAddress("localhost", 11111)));
        client = builder.build();

        // настройка системы
        String key = randomString();
        int initialValue = randomInt();
        int amount = randomInt();

        // вызов системы
        Throwable oe = null;
        try {
            client.inc(key, DEFAULT_EXP, amount, initialValue).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            oe = e.getCause();
        }

        // проверка утверждений
        Assert.assertNotNull(oe);
    }

    @Test
    public void retryOnFailStrategy() throws Exception {
        ClientBuilder builder = new ClientBuilder()
                .setTimeout(2000)
                .retryOnFail(3, 50);
        builder.addReadWriteMirror(Arrays.asList(new InetSocketAddress("localhost", 11311)));
        builder.addReadWriteMirror(Arrays.asList(new InetSocketAddress("localhost", 11211)));
        client = builder.build();

        // настройка системы
        String key = randomString();
        String value = randomString();
        client.set(key, DEFAULT_EXP, value);

        // вызов системы
        String result = client.get(key).get(3, TimeUnit.SECONDS);

        // проверка утверждений
        Assert.assertEquals(value, result);
    }

    @Test
    public void setAndGet() throws Exception {
        // настройка системы
        String key = randomString();
        String value = randomString();

        client.set(key, DEFAULT_EXP, value).get(2, TimeUnit.SECONDS);

        // вызов системы
        String result = client.get(key).get(2, TimeUnit.SECONDS);

        // проверка результатов
        Assert.assertEquals(value, result);
    }

    @Test
    public void setQ() throws Exception {
        // настройка системы
        String key = randomString();
        String value = randomString();

        // вызов системы
        client.setQ(key, DEFAULT_EXP, value);

        // проверка утверждений
        Thread.sleep(10);
        String result = client.get(key).get(1, TimeUnit.SECONDS);
        Assert.assertEquals(value, result);
    }

    @Before
    public void setUp() {
        ClientBuilder builder = new ClientBuilder()
                .setTimeout(500000);
        builder.addReadWriteMirror(Arrays.asList(new InetSocketAddress("localhost", 11211)));
        builder.addWriteOnlyMirror(Arrays.asList(new InetSocketAddress("localhost", 11311)));
        client = builder.build();
    }

    private int randomInt() {
        return new Random().nextInt(5000);
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
