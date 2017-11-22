package com.youzidata.util;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//http://blog.csdn.net/wuliu_forever/article/details/53389483
public class ZkIDUtil {
    private final static Logger Log = LoggerFactory.getLogger(ZkIDUtil.class);

    private ZkClient client = null;
    // 服务地址
    private final String server;
    // id生成器根节点
    private final String root;
    // id节点
    private final String nodeName;
    // 启动状态: true:启动;false:没有启动，默认没有启动
    private volatile boolean running = false;
    private ExecutorService cleanExector = null;

    public enum RemoveMethod {
        // 不，立即，延期
        NONE, IMMEDIATELY, DELAY
    }

    public ZkIDUtil(String zkServer, String root, String nodeName) {
        this.server = zkServer;
        this.root = root;
        this.nodeName = nodeName;
    }

    /**
     * 启动
     *
     * @version 2016年11月29日上午9:37:36
     * @author wuliu
     * @throws Exception
     */
    public void start() throws Exception {
        if (running)
            throw new Exception("server has stated...");
        running = true;
        init();
    }

    /**
     * 停止服务
     *
     * @version 2016年11月29日上午9:45:38
     * @author wuliu
     * @throws Exception
     */
    public void stop() throws Exception {
        if (!running)
            throw new Exception("server has stopped...");
        running = false;
        freeResource();
    }

    private void init() {
        client = new ZkClient(server, 5000, 5000, new BytesPushThroughSerializer());
        cleanExector = Executors.newFixedThreadPool(10);
        try {
            client.createPersistent(root, true);
        }
        catch (ZkNodeExistsException e) {
            Log.info("节点已经存在,节点路径:" + root);
        }

    }

    /**
     * 资源释放 T
     *
     * @version 2016年11月29日上午9:38:59
     * @author wuliu
     */
    private void freeResource() {
        cleanExector.shutdown();
        try {
            cleanExector.awaitTermination(2, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            cleanExector = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }

    /**
     * 判断是否启动服务
     *
     * @version 2016年11月29日上午9:39:58
     * @author wuliu
     * @throws Exception
     */
    private void checkRunning() throws Exception {
        if (!running)
            throw new Exception("请先调用start启动服务");
    }

    /**
     * 提取ID
     *
     * @version 2016年11月29日上午9:46:48
     * @author wuliu
     * @param str
     * @return
     */
    private String ExtractId(String str) {
        System.out.println("str=" + str);
        int index = str.lastIndexOf(nodeName);// 20
        System.out.println("index=" + index);
        if (index >= 0) {
            index += nodeName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    /**
     * 获取id
     *
     * @version 2016年11月29日上午9:40:33
     * @author wuliu
     * @param removeMethod
     * @return
     * @throws Exception
     */
    public String generateId(RemoveMethod removeMethod) throws Exception {
        checkRunning();
        final String fullNodePath = root.concat("/").concat(nodeName);
        // 创建顺序节点每个父节点会为他的第一级子节点维护一份时序，会记录每个子节点创建的先后顺序。
        // 基于这个特性，在创建子节点的时候，可以设置这个属性，那么在创建节点过程中，
        // ZooKeeper会自动为给定节点名加上一个数字后缀，作为新的节点名
        final String ourPath = client.createPersistentSequential(fullNodePath, null);
        if (removeMethod.equals(RemoveMethod.IMMEDIATELY)) {// 立即删除
            client.delete(ourPath);
        }
        else if (removeMethod.equals(RemoveMethod.DELAY)) {// 延期删除
            cleanExector.execute(new Runnable() {
                public void run() {
                    client.delete(ourPath);
                }
            });
        }
        return ExtractId(ourPath);
    }



}
