package com.student.works.rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class MyInterfanceImpl implements MyInterface {
    public long value = 20210123456789L;
    public String name = "心心";
    @Override
    public String findName(long studentId) {
        System.out.println("这里是查询的学号：" + studentId);
        if (studentId == value){
            return name;
        }
        return null;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return MyInterface.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
