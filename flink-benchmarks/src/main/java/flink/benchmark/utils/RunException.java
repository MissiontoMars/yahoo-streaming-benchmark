package flink.benchmark.utils;

/**
 * Alipay.com Inc Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author eagle on 2018/1/2.
 */
public class RunException extends RuntimeException {

    public RunException(Throwable t) {
        super(t);
    }

    public RunException(String msg) {
        super(msg);
    }

    public RunException(String msg, Throwable t) {
        super(msg, t);
    }
}
