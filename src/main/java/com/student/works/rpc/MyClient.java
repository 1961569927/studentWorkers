package com.student.works.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyClient {
    public static void main(String[] args) {

        try {
            MyInterface proxy = RPC.getProxy(MyInterface.class, 1L,new InetSocketAddress("127.0.0.1",12345),new Configuration());
            String res = proxy.findName(20210123456789L);
            System.out.println("第一次查询结果：" + res);
            String res2 = proxy.findName(20210000000000L);
            System.out.println("第二次查询结果：" + res2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
