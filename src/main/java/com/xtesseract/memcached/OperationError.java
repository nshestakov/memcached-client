package com.xtesseract.memcached;

/**
 * Ошибка полученная от memcached
 */
public class OperationError extends RuntimeException {

    private static String statusDescription(int status) {
        switch (status) {
            case 0x01:
                return "Key not found";
            case 0x02:
                return "Key exists";
            case 0x03:
                return "Value too large";
            case 0x04:
                return "Invalid arguments";
            case 0x05:
                return "Item not stored";
            case 0x06:
                return "Incr/Decr on non-numeric value.";
            case 0x81:
                return "Unknown command";
            case 0x82:
                return "Out of memory";
            default:
                return "Unknown status " + status;
        }
    }

    private final int status;

    public OperationError(int status) {
        super(statusDescription(status));
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
