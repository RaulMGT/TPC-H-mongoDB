package com.sm.mongo;

import java.util.*;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.*;
import org.bson.*;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class JavaMongoConnection {

	private Random random = new Random();
    private List<Integer> orderkeylist = new ArrayList<Integer>();
    private List<Integer> suppkeylist = new ArrayList<Integer>();
    private List<Integer> custkeylist = new ArrayList<Integer>();
    private List<Integer> nationkeylist = new ArrayList<Integer>();
    private List<Integer> regionkeylist = new ArrayList<Integer>();
    private List<Integer> partkeylist = new ArrayList<Integer>();

    public JavaMongoConnection() {

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

//        SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
//                sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as
//        sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
//        avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount)
//        as avg_disc, count(*) as count_order
//        FROM lineitem
//        WHERE l_shipdate <= '[date]'
//        GROUP BY l_returnflag, l_linestatus
//        ORDER BY l_returnflag, l_linestatus;

        AggregateIterable<Document> output1 = collectionLine.aggregate(Arrays.asList(
                Aggregates.match(Filters.lte("l_shipdate", getRandomDate())),
                Aggregates.project(Projections.include("l_returnflag","l_linestatus")),
                Aggregates.project(Projections.computed("sum_qty", new Document("$sum","l_quantity"))),
                Aggregates.project(Projections.computed("sum_base_price", new Document("$sum","l_extendedprice"))),
                Aggregates.project(Projections.computed("sum_disc_price", new Document("$sum", new Document("$multiply", Arrays.asList(new Document("$subtract", Arrays.asList(1L, "$l_discount")), "l_extendedprice"))))),
                Aggregates.project(Projections.computed("sum_charge", new Document("$sum", Arrays.asList(1L, "$l_tax")))),
                Aggregates.project(Projections.computed("avg_qty", new Document("$avg","l_quantity"))),
                Aggregates.project(Projections.computed("avg_price", new Document("$avg","l_extendedprice"))),
                Aggregates.project(Projections.computed("avg_disc", new Document("$avg","l_discount"))),
                Aggregates.group(Arrays.asList("l_returnflag","l_linestatus"), Accumulators.sum("count_order", 1)),
                Aggregates.sort(Sorts.ascending("l_returnflag","l_linestatus"))));

        //Query 2.
//
//        SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone,
//                s_comment
//        FROM part, supplier, partsupp, nation, region
//        WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = [SIZE]
//        AND p_type like '%[TYPE]' AND s_nationkey = n_nationkey AND n_regionkey =
//                r_regionkey AND r_name = '[REGION]' AND ps_supplycost = (SELECT
//        min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey =
//                ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND
//                n_regionkey = r_regionkey AND r_name = '[REGION]')
//        ORDER BY s_acctbal desc, n_name, s_name, p_partkey;

        AggregateIterable<Document> output2 = collectionPart.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("p_partkey", "ps_suppkey")),
                Aggregates.match(Filters.eq("l_shipdate", getRandomDate())),
                //Aggregates.match(Filters.type("p_type", "type")),
                Aggregates.match(Filters.eq("s_nationkey", "r_regionkey")),
                Aggregates.match(Filters.eq("r_name", getRandomString(15))),
                Aggregates.project(Projections.include("s_acctbal","s_name","n_name","p_partkey","p_mfgr","s_address","s_phone","s_comment")),
                Aggregates.sort(Sorts.ascending("n_name","s_name","p_partkey")),
                Aggregates.sort(Sorts.descending("s_acctbal"))
               /* Aggregates.match(Filters.eq("p_partkey", "ps_suppkey")),
                * Aggregates.match(Filters.eq("r_name", getRandomString(15))),
                *Aggregates.match(Filters.eq("s_nationkey", "r_regionkey")),
                * Aggregates.project(Projection.include("ps_supplycost"))
                *
                */
        ));

        //Query 3.

//        SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,
//                o_orderdate, o_shippriority
//        FROM customer, orders, lineitem
//        WHERE c_mktsegment = '[SEGMENT]' AND c_custkey = o_custkey AND l_orderkey =
//                o_orderkey AND o_orderdate < '[DATE1]' AND l_shipdate > '[DATE2]'
//        GROUP BY l_orderkey, o_orderdate, o_shippriority
//        ORDER BY revenue desc, o_orderdate;


        AggregateIterable<Document> output3 = collectionLine.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("c_mtksegment", getRandomString(10))),
                Aggregates.match(Filters.lt("o_orderdate", getRandomDate())),
                Aggregates.match(Filters.gt("l_shipdate", getRandomDate())),
                Aggregates.match(Filters.eq("c_custkey", "o_custkey")),
                Aggregates.match(Filters.eq("l_orderkey", "o_orderkey")),
                Aggregates.project(Projections.include("l_orderkey", "o_orderdate", "o_shippriority")),
                Aggregates.project(Projections.computed("revenue", new Document("$subtract", Arrays.asList(1L, "$l_discount")))),
                Aggregates.group(Arrays.asList("$l_orderkey", "$o_orderdate", "$o_shippriority")),
                Aggregates.sort(Sorts.descending("revenue")),
                Aggregates.sort(Sorts.ascending("o_orderdate"))
        ));
//
//        //Query 4.

//        SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
//        FROM customer, orders, lineitem, supplier, nation, region
//        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey =
//                s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND
//                n_regionkey = r_regionkey AND r_name = '[REGION]' AND o_orderdate >= date
//        '[DATE]' AND o_orderdate < date '[DATE]' + interval '1' year
//        GROUP BY n_name
//        ORDER BY revenue desc;

        Date timeBefore = getRandomDate();
        Calendar c = Calendar.getInstance();
        c.setTime(timeBefore);
        c.add(Calendar.YEAR, 1);
        Date timeAfter = c.getTime();

        AggregateIterable<Document> output4 = collectionLine.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("r_name", getRandomString(3))),
                Aggregates.match(Filters.gte("o_orderdate", timeBefore)),
                Aggregates.match(Filters.lt("o_orderdate", timeAfter)),
                Aggregates.project(Projections.include("n_name")),
                Aggregates.project(Projections.computed("revenue", new Document("$sum", new Document("$multiply", Arrays.asList(new Document("$subtract", Arrays.asList(1L, "$l_discount")), "l_extendedprice"))))),
                Aggregates.group("n_name"),
                Aggregates.sort(Sorts.descending("revenue"))
        ));

        System.out.println("QUERY 1");
        for (Document dbObject : output1) {
            System.out.println("=========================");
            System.out.println(dbObject);
        }

        System.out.println("QUERY 2");

        for (Document dbObject : output2) {
            System.out.println(dbObject);
        }

        System.out.println("QUERY 3");
        for (Document dbObject : output3) {
            System.out.println(dbObject.toJson());
            System.out.println(dbObject);
        }
        System.out.println("QUERY 4");

        for (Document dbObject : output4) {
            System.out.println(dbObject);
        }
        mongoClient.close();

    }

    private int getRandomInteger() {
        return random.nextInt(100000 - 1000) + 1000;
    }

    private int getRandomDiscount() {
        return random.nextInt(1001) / 1000;
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
        for (int i = 1; i <= 10000; ++i) {
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
        List<Document> LineItem = new ArrayList<Document>();
        for (int i = 1; i <= 10000; ++i) {
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
            document.put("l_discount", getRandomDiscount());
            document.put("l_orderkey", orderkey);
            document.put("l_suppkey", suppkey);
            document.put("n_name", getRandomString(15));
            document.put("o_custkey", custkey);
            document.put("c_nationkey", nationkey);
            document.put("n_regionkey", regionkey);
            document.put("r_name", getRandomString(3));
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
