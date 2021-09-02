package com.lsmt.test;

import java.util.Scanner;

import com.lsmt.service.ChinaDB;

/**
 * chinaDB 测试
 *
 * @author liyunfei
 * Created on 2021-08-23
 */
public class ChainDBTest {

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        ChinaDB chinaDB = new ChinaDB("/Users/xxx/chinadb/", 3, 2);
        while (true) {
            System.out.print(">> ");
            String command = sc.nextLine();
            if ("exit".equals(command.trim())) {
                break;
            }
            chinaDB.execute(command);
        }
    }
}
