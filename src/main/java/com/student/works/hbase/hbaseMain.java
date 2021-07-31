package com.student.works.hbase;

import java.io.IOException;

public class hbaseMain {
    public static void main(String[] args) throws IOException {
        HbaseUtils utils = new HbaseUtils();
        String tableName = "Diyukun:student_diyukun";
        String info = "info";
        String score = "score";
        String student_id = "student_id";
        String class_1 = "class";
        String understanding = "understanding";
        String programming = "programming";
        String[] columns = new String[]{info,score};


        //创建表
        utils.createTable("Diyukun:student_diyukun",columns);

        System.out.println("++++++++++++++++++++++++++ 创建成功");

        //插入数据
        //Tom
        String[] tomInfoColumns = new String[]{student_id,class_1};
        String[] tomInfoValues = new String[]{"20210000000001","1"};
        String[] tomScoreColumns = new String[]{understanding,programming};
        String[] tomScoreValues = new String[]{"75","82"};
        utils.insertData(tableName,"Tom",info, tomInfoColumns,tomInfoValues);
        utils.insertData(tableName,"Tom",score, tomScoreColumns,tomScoreValues);
        //Jerry
        String[] jerryInfoColumns = new String[]{student_id,class_1};
        String[] jerryInfoValues = new String[]{"20210000000001","1"};
        String[] jerryScoreColumns = new String[]{understanding,programming};
        String[] jerryScoreValues = new String[]{"85","67"};
        utils.insertData(tableName,"Jerry",info, tomInfoColumns,tomInfoValues);
        utils.insertData(tableName,"Jerry",score, tomScoreColumns,tomScoreValues);
        //Jack
        String[] jackInfoColumns = new String[]{student_id,class_1};
        String[] jackInfoValues = new String[]{"20210000000002","1"};
        String[] jackScoreColumns = new String[]{understanding,programming};
        String[] jackScoreValues = new String[]{"80","80"};
        utils.insertData(tableName,"Jack",info, tomInfoColumns,tomInfoValues);
        utils.insertData(tableName,"Jack",score, tomScoreColumns,tomScoreValues);
        //Rose
        String[] roseInfoColumns = new String[]{student_id,class_1};
        String[] roseInfoValues = new String[]{"20210000000003","2"};
        String[] roseScoreColumns = new String[]{understanding,programming};
        String[] roseScoreValues = new String[]{"60","61"};
        utils.insertData(tableName,"Rose",info, tomInfoColumns,tomInfoValues);
        utils.insertData(tableName,"Rose",score, tomScoreColumns,tomScoreValues);
        //dyk
        String[] dykInfoColumns = new String[]{student_id,class_1};
        String[] dykInfoValues = new String[]{"G20210735010123","2"};
        String[] dykScoreColumns = new String[]{understanding,programming};
        String[] dykScoreValues = new String[]{"100","100"};
        utils.insertData(tableName,"dyk",info, tomInfoColumns,tomInfoValues);
        utils.insertData(tableName,"dyk",score, tomScoreColumns,tomScoreValues);

        System.out.println("++++++++++++++++++++++++++ 插入数据成功");

        System.out.println("++++++++++++++++++++++++++ 查询全表数据开始");
        //scan 全表：
        utils.scanTable(tableName);
        System.out.println("++++++++++++++++++++++++++ 查询全表数据结束");


        System.out.println("++++++++++++++++++++++++++ 查询单row数据开始");
        //scan row
        utils.scanRow(tableName,"dyk");
        System.out.println("++++++++++++++++++++++++++ 查询单row数据结束");

        //删除数据
        utils.deleteData(tableName,"dyk");

        System.out.println("++++++++++++++++++++++++++ 删除单row数据结束");

        //删除完成后再查询一次
        //scan 全表：
        utils.scanTable(tableName);
        //scan row
        utils.scanRow(tableName,"dyk");


    }
}
