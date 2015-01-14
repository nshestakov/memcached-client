package com.xtesseract.memcached;

import io.netty.util.concurrent.Promise;

/**
 * Простой асинхронный memcached клиент
 */
public interface Client {

    /**
     * Добавляет значение в кеш если оно еще не задано. Подтверждения успешного выполнения не дожидается.
     *
     * @param key   ключ с которым должно быть добавлено значение
     * @param exp   время жизни значения в сек.
     * @param value добавляеое значение
     */
    void addQ(String key, int exp, String value);

    /**
     * Уменьшает значение в кеше на 1.
     *
     * @param key ключ, значение которого уменьшается
     * @param exp время жизни значения в сек.
     * @return новое значение ключа
     */
    Promise<Long> dec(String key, int exp);

    /**
     * Уменьшает значение в кеше на {@code incValue}. Если ключ не существует, то устанавливает значение в
     * {@code initialValue}.
     *
     * @param key ключ, значение которого уменьшается
     * @param exp время жизни значения в сек.
     * @return новое значение ключа
     */
    Promise<Long> dec(String key, int exp, long incValue, long initialValue);

    /**
     * Уменьшает значение в кеше на {@code incValue}. Если ключ не существует, то устанавливает значение в
     * {@code initialValue}. Подтверждения успешного выполнения не дожидается.
     *
     * @param key ключ, значение которого уменьшается
     * @param exp время жизни значения в сек.
     */
    void decQ(String key, int exp, long incValue, long initialValue);

    /**
     * Удаляет из кеша значение.
     *
     * @param key ключ, значение которого удаляется
     * @return
     */
    Promise<Void> delete(String key);

    /**
     * Удаляет из кеша значение. Подтверждения успешного выполнения не дожидается.
     *
     * @param key ключ, значение которого удаляется
     */
    void deleteQ(String key);

    /**
     * Возвращает значение соответствующее ключу.
     *
     * @param key ключ получемого значения
     * @return значение ключа
     */
    Promise<String> get(String key);

    /**
     * Увеличивает значение в кеше на 1.
     *
     * @param key ключ, значение которого увеличивается
     * @param exp время жизни значения в сек.
     * @return новое значение ключа
     */
    Promise<Long> inc(String key, int exp);

    /**
     * Увеличивает значение в кеше на {@code incValue}. Если ключ не существует, то устанавливает значение в
     * {@code initialValue}.
     *
     * @param key ключ, значение которого увеличивается
     * @param exp время жизни значения в сек.
     * @return новое значение ключа
     */
    Promise<Long> inc(String key, int exp, long incValue, long initialValue);

    /**
     * Увеличивает значение в кеше на {@code incValue}. Если ключ не существует, то устанавливает значение в
     * {@code initialValue}. Подтверждения успешного выполнения не дожидается.
     *
     * @param key ключ, значение которого увеличивается
     * @param exp время жизни значения в сек.
     */
    void incQ(String key, int exp, long incValue, long initialValue);

    /**
     * Заменяет значение. Если ранее значения не было установлено, то ничего не делает. Подтверждения успешного
     * выполнения не дожидается.
     *
     * @param key   ключ, значение которого изменяется
     * @param exp   exp время жизни значения в сек.
     * @param value устанавливаемое значение
     */
    void replaceQ(String key, int exp, String value);

    /**
     * Устанавливает значение.
     *
     * @param key   ключ, значение которого изменяется
     * @param exp   exp время жизни значения в сек.
     * @param value устанавливаемое значение
     * @return
     */
    Promise<Void> set(String key, int exp, String value);

    /**
     * Устанавливает значение. Подтверждения успешного выполнения не дожидается.
     *
     * @param key   ключ, значение которого изменяется
     * @param exp   exp время жизни значения в сек.
     * @param value устанавливаемое значение
     */
    void setQ(String key, int exp, String value);
}
