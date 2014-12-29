package com.xtesseract.memcached;

/**
 * <list hangIndent="8" style="hanging">
 * <t hangText="0x00">Get</t>
 * <t hangText="0x01">Set</t>
 * <t hangText="0x02">Add</t>
 * <t hangText="0x03">Replace</t>
 * <t hangText="0x04">Delete</t>
 * <t hangText="0x05">Increment</t>
 * <t hangText="0x06">Decrement</t>
 * <t hangText="0x07">Quit</t>
 * <t hangText="0x08">Flush</t>
 * <t hangText="0x09">GetQ</t>
 * <t hangText="0x0A">No-op</t>
 * <t hangText="0x0B">Version</t>
 * <t hangText="0x0C">GetK</t>
 * <t hangText="0x0D">GetKQ</t>
 * <t hangText="0x0E">Append</t>
 * <t hangText="0x0F">Prepend</t>
 * <t hangText="0x10">Stat</t>
 * <t hangText="0x11">SetQ</t>
 * <t hangText="0x12">AddQ</t>
 * <t hangText="0x13">ReplaceQ</t>
 * <t hangText="0x14">DeleteQ</t>
 * <t hangText="0x15">IncrementQ</t>
 * <t hangText="0x16">DecrementQ</t>
 * <t hangText="0x17">QuitQ</t>
 * <t hangText="0x18">FlushQ</t>
 * <t hangText="0x19">AppendQ</t>
 * <t hangText="0x1A">PrependQ</t>
 * </list>
 */
public class Operation {
    public static final byte GET = (0x00);
    public static final byte SET = (0x01);
    public static final byte ADD = (0x02);
    public static final byte REPLACE = (0x03);
    public static final byte DELETE = (0x04);
    public static final byte INCREMENT = (0x05);
    public static final byte DECREMENT = (0x06);
    public static final byte QUIT = (0x07);
    public static final byte FLUSH = (0x08);
    public static final byte GET_Q = (0x09);
    public static final byte NOOP = (0x0A);
    public static final byte VERSION = (0x0B);
    public static final byte GET_K = (0x0C);
    public static final byte GET_KQ = (0x0D);
    public static final byte APPEND = (0x0E);
    public static final byte PREPEND = (0x0F);
    public static final byte STAT = (0x10);
    public static final byte SET_Q = (0x11);
    public static final byte ADD_Q = (0x12);
    public static final byte REPLACE_Q = (0x13);
    public static final byte DELETE_Q = (0x14);
    public static final byte INCREMENT_Q = (0x15);
    public static final byte DECREMENT_Q = (0x16);
    public static final byte QUIT_Q = (0x17);
    public static final byte FLUSH_Q = (0x18);
    public static final byte APPEND_Q = (0x19);
    public static final byte PREPEND_Q = (0x1A);
}
