package com.lsmt.component;

import java.io.RandomAccessFile;

import lombok.Data;

/**
 * @author liyunfei
 * Created on 2021-08-26
 */
@Data
public class MetaInfo {
    /**
     * 数据长度
     */
    private long dataLength;
    /**
     * 数据开始位置
     */
    private long dataStart;
    /**
     * 稀疏索引文件长度
     */
    private long sparseLength;
    /**
     * 稀疏索引开始位置
     */
    private long sparseStart;
    /**
     * 分段大小
     */
    private long partSize;

    public void saveFile(RandomAccessFile ssTable) {
        try {
            ssTable.writeLong(partSize);
            ssTable.writeLong(dataLength);
            ssTable.writeLong(dataStart);
            ssTable.writeLong(sparseLength);
            ssTable.writeLong(sparseStart);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static MetaInfo readFromFile(RandomAccessFile ssTable) {
        try {
            MetaInfo metaInfo = new MetaInfo();
            ssTable.seek(ssTable.length() - 8);
            metaInfo.sparseStart = ssTable.readLong();
            ssTable.seek(ssTable.length() - 8 * 2);
            metaInfo.sparseLength = ssTable.readLong();
            ssTable.seek(ssTable.length() - 8 * 3);
            metaInfo.dataStart = ssTable.readLong();
            ssTable.seek(ssTable.length() - 8 * 4);
            metaInfo.dataLength = ssTable.readLong();
            ssTable.seek(ssTable.length() - 8 * 5);
            metaInfo.partSize = ssTable.readLong();
            return metaInfo;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
