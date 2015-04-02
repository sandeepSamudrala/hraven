package com.twitter.hraven.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

public class CellRecords {

    public static byte[] getValueBytes(Cell cell){
        byte[] cellValueBytes = Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        return cellValueBytes;
    }

    public static long getValueLong(Cell cell){
        return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    public static int getValueInt(Cell cell){
        return Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    public static String getValueString(Cell cell){
        return Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    }

    public static byte[] getFamilyBytes(Cell cell){
        byte[] cellFamilyBytes = Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
        return cellFamilyBytes;
    }

    public static byte[] getQualifierBytes(Cell cell){
        byte[] cellQualifierBytes = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        return cellQualifierBytes;
    }

    public static String getQualifierString(Cell cell){
        return Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

    }
}
