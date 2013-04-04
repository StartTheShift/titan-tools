package com.shift.titantools;

/**
 * Created with IntelliJ IDEA.
 * User: bdeggleston
 * Date: 4/2/13
 * Time: 5:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class RepairException extends Exception {
    public RepairException() { }
    public RepairException(String s) { super(s); }
    public RepairException(String s, Throwable throwable) { super(s, throwable); }
    public RepairException(Throwable throwable) { super(throwable); }
}
