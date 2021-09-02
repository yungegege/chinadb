package com.lsmt.component;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.gson.Gson;
import com.lsmt.model.DataModel;
import com.lsmt.model.Position;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * 磁盘上的表，有序
 *
 * @author liyunfei
 * Created on 2021-08-23
 */
@Slf4j
@Data
public class SsTable {


    private static Gson gson = new Gson();

    public static final String RW_MODE = "rw";


    /**
     * 数据目录
     */
    private String dataDir;

    /**
     * 分段大小
     */
    private int partitionSize;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile ssTable;

    /**
     * 暂存数据日志文件
     */
    private File ssTableFile;

    /**
     * 表的结构信息
     */
    private MetaInfo metaInfo;


    /**
     * 稀疏索引
     *
     * @param dataDir
     */
    private TreeMap<String, Position> /**/sparseIndex;

    public SsTable(String filePath, int partitionSize) {
        try {
            this.partitionSize = partitionSize;
            this.metaInfo = new MetaInfo();
            this.ssTableFile = new File(filePath);
            this.ssTable = new RandomAccessFile(ssTableFile, RW_MODE);
            this.sparseIndex = new TreeMap<>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    /**
     * 内存表落到磁盘
     */
    public void saveFile(TreeMap<String, DataModel> index) {
        try {
            // 遍历内存表
            JSONObject jsonObject = new JSONObject(true);
            metaInfo.setDataStart(ssTable.getFilePointer());
            for (Map.Entry<String, DataModel> entry : index.entrySet()) {
                jsonObject.put(entry.getKey(), entry.getValue());
                // 达到分段
                if (jsonObject.size() >= partitionSize) {
                    // 保存文件
                    savePart(jsonObject);
                }
            }
            // 保存剩余未达到partitionSize的键值对
            if (jsonObject.size() > 0) {
                savePart(jsonObject);
            }
            metaInfo.setDataLength(ssTable.getFilePointer() - metaInfo.getDataStart());

            // 稀释索引落盘
            byte[] bytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
            metaInfo.setSparseStart(ssTable.getFilePointer());
            ssTable.write(bytes);
            metaInfo.setSparseLength(bytes.length);
            // 表文数据落盘
            metaInfo.saveFile(ssTable);

        } catch (Exception e) {
            throw new RuntimeException();
        }

    }

    /**
     * 分段保存文件
     */
    private void savePart(JSONObject partData) throws Exception {
        byte[] bytes = partData.toJSONString().getBytes();
        long start = ssTable.getFilePointer();
        ssTable.write(bytes);

        //记录数据段的第一个key到稀疏索引中
        Optional<String> firstKey = partData.keySet().stream().findFirst();
        firstKey.ifPresent(s -> sparseIndex.put(s, new Position(start, bytes.length)));
        partData.clear();
    }

    /**
     * 查询key
     */
    public String query(String key) {
        try {
            // 查找最后一个比key小的
            boolean first = true;
            Position lastSmall = null;
            for (Entry<String, Position> entry : sparseIndex.entrySet()) {
                // 如果第一个就比key大，返回null
                if (first && entry.getKey().compareTo(key) > 0) {
                    return null;
                }
                first = false;
                // k>key
                if (entry.getKey().compareTo(key) <= 0) {
                    lastSmall = entry.getValue();
                } else {
                    break;
                }
            }
            // 读一段
            ssTable.seek(lastSmall.getStart());
            byte[] bytes = new byte[(int) lastSmall.getLength()];
            ssTable.read(bytes);
            String str = new String(bytes, StandardCharsets.UTF_8);
            JSONObject partData = JSONObject.parseObject(str);
            JSONObject jsonObject = partData.getJSONObject(key);
            if (jsonObject == null) {
                return null;
            }
            // 转为DataModel
            DataModel dataModel = jsonObject.toJavaObject(DataModel.class);
            return dataModel.getDelete() == 1 ? null : dataModel.getValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 文件恢复到内存
     */
    public void restoreFromFile() {
        try {
            // 读取元信息
            metaInfo = MetaInfo.readFromFile(ssTable);
            // 读稀疏索引
            ssTable.seek(metaInfo.getSparseStart());
            byte[] bytes = new byte[(int) metaInfo.getSparseLength()];
            ssTable.read(bytes);
            String str = new String(bytes, StandardCharsets.UTF_8);
            sparseIndex = JSONObject.parseObject(str,
                    new TypeReference<TreeMap<String, Position>>() {
                    });
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public static void main(String[] args) {
        TreeMap<String, String> sparseIndex = new TreeMap<>();
        sparseIndex.put("b", "b");
        sparseIndex.put("c", "c");
        sparseIndex.put("a", "a");
        sparseIndex.put("f", "f");
        sparseIndex.put("d", "d");
        for (String key : sparseIndex.keySet()) {
            System.out.println(sparseIndex.get(key));
        }
    }
}
