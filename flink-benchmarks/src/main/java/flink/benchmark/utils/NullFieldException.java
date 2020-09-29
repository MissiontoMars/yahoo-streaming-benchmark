package flink.benchmark.utils;

/**
 * Alipay.com Inc Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author gaolun on 18/1/18.
 */
public class NullFieldException extends RunException {

    public NullFieldException(int pos) {
        super("pos" + pos + " is null");
    }
}
