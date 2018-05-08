package com.sm.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class JavaMongoConnection {

	 private Random random = new Random();
    private List<Integer> orderkeylist = new ArrayList<Integer>();
    private List<Integer> suppkeylist = new ArrayList<Integer>();
    private List<Integer> custkeylist = new ArrayList<Integer>();
    private List<Integer> nationkeylist = new ArrayList<Integer>();
    private List<Integer> regionkeylist = new ArrayList<Integer>();
    private List<Integer> partkeylist = new ArrayList<Integer>();
    private MongoDatabase database;

    public JavaMongoConnection(){

    }

    public static void main(String args[]) {
        // Connecting to the server

        JavaMongoConnection jmc = new JavaMongoConnection();
        jmc.run();
    }

    private void run() {

        MongoClient mongoClient = new MongoClient("localhost", 27017);
        System.out.println("server connection succesfully done");

        // Connecting with database
        MongoDatabase database = mongoClient.getDatabase("test");
        database.drop();
        Map<MongoCollection<Document>, List<Document>> inserts = new HashMap<MongoCollection<Document>, List<Document>>();
        MongoCollection<Document> collectionLine = database.getCollection("LineItem");
        MongoCollection<Document> collectionPart = database.getCollection("PartSupp");
        inserts.put(collectionLine, getLineItemObjects());
        inserts.put(collectionPart, getPartSuppObjects());

        for (MongoCollection<Document> collection : inserts.keySet()) {
            List<Document> individualInserts = inserts.get(collection);
            for (Document object : individualInserts) {
                collection.insertOne(object);
            }
        }
        //Query 1.
        AggregateIterable<Document> output1 = collectionLine.aggregate(Arrays.asList(
                new Document("$match", new Document("l_extendedprice", getRandomInteger()))));

        //Query 2.
//        AggregateIterable<Document> output2 = collectionPart.aggregate(Arrays.asList(
//                new Document("$match", new Document("l_extendedprice", 5))));
//
//        //Query 3.
//        AggregateIterable<Document> output3 = collectionLine.aggregate(Arrays.asList(
//                new Document("$match", new Document("l_extendedprice", 5))));
//
//        //Query 4.
//        AggregateIterable<Document> output4 = collectionLine.aggregate(Arrays.asList(
//                new Document("$match", new Document("l_extendedprice", 5))));

        System.out.println("QUERY 1");

        for(Document dbObject : output1){
            System.out.println(dbObject);
        }

//        System.out.println("QUERY 2");
//
//        for(Document dbObject : output2){
//            System.out.println(dbObject);
//        }
//
//        System.out.println("QUERY 3");
//
//        for(Document dbObject : output3){
//            System.out.println(dbObject);
//        }
//
//        System.out.println("QUERY 4");
//
//        for(Document dbObject : output4){
//            System.out.println(dbObject);
//        }
        // mongoClient.close();

    }

    private int getRandomInteger() {
        return random.nextInt(100000 - 1000) + 1000;
    }

    private String getRandomString(int size) {
        String result = "";
        for (int i = 0; i < size / 2; ++i) {
            int number = random.nextInt(20);
            char c = (char) ('a' + number);
            result += c;
        }
        return result;
    }

    private Date getRandomDate() {
        Calendar calendar = new GregorianCalendar();
        calendar.set(2018, 5, 9);
        calendar.add(Calendar.DAY_OF_YEAR, random.nextInt(10000) - 5000);
        return calendar.getTime();
    }

    private List<Document> getPartSuppObjects() {
        List<Document> PartSupp = new ArrayList<Document>();
        for (int i = 1; i <= 6000; ++i) {
            Document document = new Document();
            int partkey = getRandomInteger();
            int suppkey = getRandomInteger();
            int nationkey = getRandomInteger();
            int regionkey = getRandomInteger();
            while (partkeylist.contains(partkey))
                partkey = getRandomInteger();
            partkeylist.add(partkey);
            while (suppkeylist.contains(suppkey))
                suppkey = getRandomInteger();
            suppkeylist.add(suppkey);
            while (nationkeylist.contains(nationkey))
                nationkey = getRandomInteger();
            nationkeylist.add(nationkey);
            while (regionkeylist.contains(regionkey))
                regionkey = getRandomInteger();
            regionkeylist.add(regionkey);
            document.put("_id", String.valueOf(partkey) + String.valueOf(suppkey));
            document.put("ps_partkey", partkey);
            document.put("ps_suppkey", suppkey);
            document.put("p_size", getRandomInteger());
            document.put("p_type", getRandomString(15));
            document.put("s_nationkey", nationkey);
            document.put("n_regionkey", regionkey);
            document.put("r_name", getRandomString(15));
            document.put("ps_supplycost", getRandomInteger());
            document.put("s_acctbal", getRandomInteger());
            document.put("s_name", getRandomString(15));
            document.put("n_name", getRandomString(15));
            document.put("p_mfgr", getRandomString(15));
            document.put("s_address", getRandomString(20));
            document.put("s_phone", getRandomInteger());
            document.put("s_comment", getRandomString(25));
            PartSupp.add(document);
        }
        return PartSupp;

    }

    private List<Document> getLineItemObjects() {
        // TODO Auto-generated method stub
        List<Document> LineItem = new ArrayList<Document>();
        for (int i = 1; i <= 6000; ++i) {
            Document document = new Document();
            int orderkey = getRandomInteger();
            int suppkey = getRandomInteger();
            int custkey = getRandomInteger();
            int nationkey = getRandomInteger();
            int regionkey = getRandomInteger();
            while (orderkeylist.contains(orderkey))
                orderkey = getRandomInteger();
            orderkeylist.add(orderkey);
            while (suppkeylist.contains(suppkey))
                suppkey = getRandomInteger();
            suppkeylist.add(suppkey);
            while (custkeylist.contains(custkey))
                custkey = getRandomInteger();
            custkeylist.add(custkey);
            while (nationkeylist.contains(nationkey))
                nationkey = getRandomInteger();
            nationkeylist.add(nationkey);
            while (regionkeylist.contains(regionkey))
                regionkey = getRandomInteger();
            regionkeylist.add(regionkey);
            document.put("_id", orderkey);
            document.put("l_extendedprice", getRandomInteger());
            document.put("l_discount", getRandomInteger());
            document.put("l_orderkey", orderkey);
            document.put("l_suppkey", suppkey);
            document.put("n_name", getRandomString(15));
            document.put("o_custkey", custkey);
            document.put("c_nationkey", nationkey);
            document.put("n_regionkey", regionkey);
            document.put("r_name", getRandomString(15));
            document.put("o_orderdate", getRandomDate());
            document.put("l_quantity", getRandomInteger());
            document.put("l_tax", getRandomInteger());
            document.put("l_shipdate", getRandomDate());
            document.put("l_returnflag", getRandomString(1));
            document.put("l_linestatus", getRandomString(1));
            document.put("c_mtksegment", getRandomString(10));
            document.put("l_orderdate", getRandomDate());
            document.put("o_shippriority", getRandomInteger());

            LineItem.add(document);
        }
        return LineItem;

    }
}
