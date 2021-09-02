package com.lsmt.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.lsmt.component.SsTable;
import com.lsmt.model.DataModel;

/**
 * @author liyunfei
 * Created on 2021-08-23
 */
public class ChinaDB {

    private static Gson gson = new Gson();

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static final String WAL = "wal";
    public static final String RW_MODE = "rw";
    public static final String TABLE = ".chinadb";

    /**
     * 数据目录
     */
    private String dataDir;

    /**
     * 表数据大小(key个数)，即达到多少后落盘
     */
    private int tableSize;

    /**
     * 表分段大小(key个数)，用于建稀疏索引
     */
    private int partitionSize;

    /**
     * 当前操作的内存表
     */
    private TreeMap<String, DataModel> curIndex;

    /**
     * 不可变内存表，用于持久化内存表中时暂存数据
     */
    private TreeMap<String, DataModel> immutableIndex;

    /**
     * SsTable
     */
    private LinkedList<SsTable> ssTableList;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile wal;

    /**
     * 暂存数据日志文件
     */
    private File walFile;

    /**
     * 初始化
     *
     * @param dataDir 数据目录
     * @param tableSize 表数据大小(key个数)，即达到多少后落盘
     * @param partitionSize 表分段大小(key个数)，用于建稀疏索引
     */
    public ChinaDB(String dataDir, int tableSize, int partitionSize) {
        try {
            this.dataDir = dataDir;
            this.tableSize = tableSize;
            this.partitionSize = partitionSize;
            this.ssTableList = new LinkedList<>();
            curIndex = new TreeMap<>();
            // 遍历数据目录
            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            // 如果没文件，则建立一个WAL文件
            if (files == null || files.length == 0) {
                walFile = new File(dataDir + WAL);
                wal = new RandomAccessFile(walFile, RW_MODE);
                return;
            }
            TreeMap<Long, SsTable> tableTreeMap = new TreeMap<>(Comparator.reverseOrder());
            for (File file : files) {
                // 从wal恢复数据
                if (file.getName().endsWith(WAL)) {
                    walFile = file;
                    wal = new RandomAccessFile(walFile, RW_MODE);
                    restoreByWal();
                }

                // 加载sstable的元信息、稀疏索引到内存，用于查询
                if (file.getName().endsWith(TABLE)) {
                    int index = file.getName().lastIndexOf(".");
                    SsTable ssTable = new SsTable(file.getAbsolutePath(), partitionSize);
                    ssTable.restoreFromFile();
                    tableTreeMap.put(Long.parseLong(file.getName().substring(0, index)), ssTable);
                }

                ssTableList.addAll(tableTreeMap.values());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从WAL文件恢复数据
     */
    private void restoreByWal() {
        try {
            int off = 0;
            wal.seek(off);
            while (off < wal.length()) {
                // int是这个键值对占用的字节个数，跟写入的规则一致
                int length = wal.readInt();
                byte[] bytes = new byte[length];
                wal.read(bytes);
                // 放入树中
                DataModel dataModel = gson.fromJson(new String(bytes, StandardCharsets.UTF_8), DataModel.class);
                if (dataModel != null) {
                    curIndex.put(dataModel.getKey(), dataModel);
                }
                off = off + length + 4;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        //        DataModel dataModel = new DataModel("a", "b");
        //        JSONObject jsonObject = JSON.parseObject(new String(JSONObject.toJSONBytes(dataModel),
        //        StandardCharsets.UTF_8));
        //        DataModel newDataModel = jsonObject.toJavaObject(DataModel.class);
        //        System.out.println(JSONObject.toJSON(newDataModel));
        for (int i = 0; i < 100; i++) {
            DataModel dataModel = new DataModel("a", "b");
            DataModel newDataModel = gson.fromJson(new String(gson.toJson(dataModel).getBytes()), DataModel.class);
            System.out.println(gson.toJson(newDataModel));
        }

    }

    /**
     * 执行命令
     */
    public void execute(String commandLine) {
        if (StringUtils.isBlank(commandLine)) {
            return;
        }
        String[] strings = StringUtils.split(commandLine, " ");
        String command = strings[0].toLowerCase();
        if ("set".equals(command)) {
            if (strings.length != 3) {
                System.out.println("(error) ERR Syntax error");
            } else {
                set(processQuotation(strings[1]), processQuotation(strings[2]));
            }
        } else if ("get".equals(command)) {
            if (strings.length != 2) {
                System.out.println("(error) ERR Syntax error");
            } else {
                get(processQuotation(strings[1]));
            }
        } else if ("del".equals(command)) {
            if (strings.length != 2) {
                System.out.println("");
            } else {
                del(processQuotation(strings[1]));
            }
        } else {
            System.out.println("(error) unsupported command");
        }
    }

    public String processQuotation(String word) {
        if (StringUtils.isBlank(word) || StringUtils.length(word) == 1) {
            return word;
        }
        if (word.startsWith("\"") && word.endsWith("\"")) {
            return word.substring(1, word.length() - 1);
        }
        return word;
    }


    /**
     * set命令
     */
    public void set(String key, String value) {
        try {
            DataModel dataModel = new DataModel(key, value);
            byte[] bytes = gson.toJson(dataModel).getBytes();
            // 先写wal
            wal.writeInt(bytes.length);
            wal.write(bytes);
            // 再写内存
            curIndex.put(key, dataModel);
            // 内存表数据达到阈值，持久化
            if (curIndex.size() >= tableSize) {
                swapAndSaveIndex();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void del(String key) {
        try {
            DataModel dataModel = new DataModel(key, 1);
            byte[] bytes = gson.toJson(dataModel).getBytes();
            // 先写wal
            wal.writeInt(bytes.length);
            wal.write(bytes);
            // 再写内存
            curIndex.put(key, dataModel);
            // 内存表数据达到阈值，持久化
            if (curIndex.size() >= tableSize) {
                swapAndSaveIndex();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 交换和保存文件
     */
    private void swapAndSaveIndex() throws FileNotFoundException {
        immutableIndex = curIndex;
        curIndex = new TreeMap<>();
        File tmpFile = new File(dataDir + "wal_tmp");
        if (tmpFile.exists()) {
            if (!tmpFile.delete()) {
                throw new RuntimeException("删除文件失败：wal_tmp");
            }
        }
        if (!walFile.renameTo(tmpFile)) {
            throw new RuntimeException("更新文件失败：wai->wal_tmp");
        }
        walFile = new File(dataDir + WAL);
        wal = new RandomAccessFile(walFile, RW_MODE);
        executorService.submit(() -> {
            SsTable ssTable = new SsTable(dataDir + System.currentTimeMillis() + TABLE, partitionSize);
            ssTable.saveFile(immutableIndex);
            ssTableList.addFirst(ssTable);
            if (tmpFile.exists()) {
                if (!tmpFile.delete()) {
                    throw new RuntimeException("删除文件失败：wal_tmp");
                }
            }
        });
    }

    public void get(String key) {
        // 先查可变内存表
        DataModel dataModel = curIndex.get(key);
        if (dataModel != null) {
            if (dataModel.getDelete() == 0) {
                System.out.println(dataModel.getValue());
            }
            return;
        }

        if (immutableIndex != null) {
            // 再查不可变内存表
            dataModel = immutableIndex.get(key);
            if (dataModel != null) {
                if (dataModel.getDelete() == 0) {
                    System.out.println(dataModel.getValue());
                }
                return;
            }
        }

        // 查不到，再查磁盘，遍历ssTable
        // 先用布隆过滤器判断表里有没有，没有就直接结束，有就继续查
        for (SsTable ssTable : ssTableList) {
            String value = ssTable.query(key);
            if (value != null) {
                System.out.println(value);
                return;
            }
        }
    }


}
