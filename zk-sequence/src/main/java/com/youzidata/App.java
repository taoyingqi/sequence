package com.youzidata;

import com.youzidata.util.ZkIDUtil;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws Exception {
        ZkIDUtil zkIDUtil = new ZkIDUtil("10.0.110.121:2181",
                "/NameService/GenerateId", "ID-");
        zkIDUtil.start();

        try {
            for (int i = 0; i < 2; i++) {
                String id = zkIDUtil.generateId(ZkIDUtil.RemoveMethod.DELAY);
                System.out.println(id);
            }
        } finally {
            zkIDUtil.stop();
        }
    }
}
