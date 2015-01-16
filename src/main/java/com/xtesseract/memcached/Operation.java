package com.xtesseract.memcached;

/**
 * Список кдов команд memcached
 *
 * @see <a href="https://github.com/memcached/memcached/blob/master/doc/protocol-binary.xml">memcached binary protocol documentation</a>
 */
public class Operation {
    public static final byte GET = 0x00;
    public static final byte SET = 0x01;
    public static final byte ADD = 0x02;
    public static final byte REPLACE = 0x03;
    public static final byte DELETE = 0x04;
    public static final byte INCREMENT = 0x05;
    public static final byte DECREMENT = 0x06;
    public static final byte QUIT = 0x07;
    public static final byte FLUSH = 0x08;
    public static final byte GET_Q = 0x09;
    public static final byte NOOP = 0x0A;
    public static final byte VERSION = 0x0B;
    public static final byte GET_K = 0x0C;
    public static final byte GET_KQ = 0x0D;
    public static final byte APPEND = 0x0E;
    public static final byte PREPEND = 0x0F;
    public static final byte STAT = 0x10;
    public static final byte SET_Q = 0x11;
    public static final byte ADD_Q = 0x12;
    public static final byte REPLACE_Q = 0x13;
    public static final byte DELETE_Q = 0x14;
    public static final byte INCREMENT_Q = 0x15;
    public static final byte DECREMENT_Q = 0x16;
    public static final byte QUIT_Q = 0x17;
    public static final byte FLUSH_Q = 0x18;
    public static final byte APPEND_Q = 0x19;
    public static final byte PREPEND_Q = 0x1A;
}
