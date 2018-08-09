package spark;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;


public class App {

    static HashMap<String,String> hm = new HashMap<>();
    static HashMap<String,String> hm2 = new HashMap<>();



    public static void main(String[] args) {
        repairDataFromParquetFileLocal(args[0]);
//        repairDataFromParquetFileLocal("/home/hung/Downloads/parquet_logfile_at_14h_00.snap");
    }


    //  Loc du lieu tu mot file parquet tren server roi ghi du lieu ra file csv gom co ip, city, region
    private static  void repairDataFromParquetFileLocal(String filePath){
        SparkSession spark = SparkSession
                .builder()
                .appName("Connect Server")
                .config("spark.sql.parquet.binaryAsString", "true")
//                .config("spark.master","local")
                .config("spark.yarn.access.hadoopFileSystems", "hdfs://192.168.23.200:9000")
                .getOrCreate();

        //Du lieu ko chuan
        //province
        HashMap<String,String> lLocation = new HashMap<>();
        lLocation.put("Bac Kan Province","Bac Kan");
        lLocation.put("Binh Dinh Province","Binh Dinh");
        lLocation.put("Bac Ninh Province","Bac Ninh");
        lLocation.put("An Giang Province","An Giang");
        lLocation.put("Binh Thuan Province","Binh Thuan");
        lLocation.put("Djak Lak Province","Dak Lak");
        lLocation.put("Djong Thap Province","Dong Thap");
        lLocation.put("Gia Lai Province","Gia Lai");
        lLocation.put("Ha Tinh Province","Ha Tinh");
        lLocation.put("Haiphong","Hai Phong");
        lLocation.put("Hanoi","Ha Noi");
        lLocation.put("Hung Yen Province","Hung Yen");
        lLocation.put("Khanh Hoa Province","Khanh Hoa");
        lLocation.put("Kon Tum Province","Kon Tum");
        lLocation.put("Lam Djong","Lam Dong");
        lLocation.put("Long An Province","Long An");
        lLocation.put("Ninh Binh Province","Ninh Binh");
        lLocation.put("Ninh Thuan Province","Ninh Thuan");
        lLocation.put("Phu Tho Province","Phu Tho");
        lLocation.put("Phu Yen Province","Phu Yen");
        lLocation.put("Quang Binh Province","Quang Binh");
        lLocation.put("Quang Nam Province","Quang Nam");
        lLocation.put("Quang Tri Province","Quang Tri");
        lLocation.put("Tay Ninh Province","Tay Ninh");
        lLocation.put("Vinh Phuc Province","Vinh Phuc");

        //city
        lLocation.put("Chau Djoc","Chau Doc");
        lLocation.put("Djien Bien Phu","Dien Bien Phu");
        lLocation.put("Djong Ha","Dong Ha");
        lLocation.put("Djong Hoi","Dong Hoi");
        lLocation.put("Phan Rangâ€“Thap Cham","Phan Rang-Thap Cham");
        lLocation.put("Quang Tri Province","Quang Tri");
        lLocation.put("Sa Djec","Sa Dec");
        lLocation.put("Tam Djiep","Tam Diep");
        lLocation.put("Nam Djinh","Nam Dinh");

        Dataset<Row> logDF = spark.read().parquet(filePath);
        Set<String> ipList = new HashSet<>();
        Dataset<Row> filteredDS = logDF.distinct().filter((FilterFunction<Row>) row -> checkIpRequirement(row.getString(0), row.getString(2)));
        JavaRDD<String> ipDS = filteredDS.select("ip").as(Encoders.STRING()).javaRDD();

        ArrayList<String> list = new ArrayList<>();

        JavaPairRDD<String, Integer> counts = ipDS
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a,b) -> a + b);
        for (Tuple2<String, Integer> s : counts.collect()) {
            if (s._2 > 2) ipList.add(s._1);
            if (s._2==1) list.add(s._1);
        }
        Dataset<Row> finalSet = filteredDS.filter((FilterFunction<Row>) row -> !checkDuplicate(row.getString(0), ipList)).sort("ip");
//        finalSet.show(1000, false);
        finalSet.createOrReplaceTempView("l1");

        Dataset<Row> list1 = spark.sql("select ip,count(ip) as count from l1 group by ip");
//        list1.show();
        Dataset<Row> data1 = list1.join(finalSet,finalSet.col("ip").equalTo(list1.col("ip"))).where("count=1");
        Dataset<Row> data2 = list1.join(finalSet,finalSet.col("ip").equalTo(list1.col("ip"))).where("count=2");


        // viet lai add hm
        data1.foreach(row -> {
            String ip = row.getString(0);
            String location = row.getString(4);
            String[] s = location.split(",");
            String key;
            if (s.length==3) {
                key = ip + ":" + s[1];
                if (hm.containsKey(key) && hm.get(key) != s[0]) hm.put(key, "-");
                else hm.put(key, s[0]);
            }
            if (s.length==2){
                key = ip + ":" + s[0];
                if (hm.containsKey(key) && hm.get(key) != s[0]) hm.put(key, "-");
                else hm.put(key, s[0]);
            }
        });
        data2.foreach(row -> {
            String ip = row.getString(0);
            String location = row.getString(4);
            String[] s = location.split(",");
            String key;
            if (s.length==3) {
                key = ip + ":" + s[1];
                if (hm2.containsKey(key) && hm2.get(key) != s[0]) hm2.put(key, "-");
                else hm2.put(key, s[0]);
            }
            if (s.length==2){
                key = ip + ":" + s[0];
                if (hm2.containsKey(key) && hm2.get(key) != s[0]) hm2.put(key, "-");
                else hm2.put(key, s[0]);
            }
        });

        hm2.forEach((ip_prov,city)->{
            if (!hm.containsKey(ip_prov))
                hm.put(ip_prov,city);
        });

        HashMap<String,String> lLocEnd = new HashMap<>();//(ip,city+pro)
        hm.forEach((key,value)->{
            String ip,city,region;
            String[] ip_pro = key.split(":");
            ip = ip_pro[0];
            city = value;
            region = ip_pro[1];
            if (lLocation.containsKey(city))
                city = lLocation.get(value);
            if (lLocation.containsKey(region))
                region = lLocation.get(ip_pro[1]);
            if (lLocEnd.containsKey(ip) && lLocEnd.get(ip)!=city+":"+region)
                lLocEnd.put(ip,"-");
            else lLocEnd.put(ip,city+":"+region);
        });

        cvsWriter(lLocEnd);

        spark.stop();
    }

    //ghi vao file cvs
    private static void cvsWriter(HashMap<String,String> hm) {
        try {
            //We have to create the CSVPrinter class object
            LocalDate today = LocalDate.now();

            // Ghi file

            Writer writer = Files.newBufferedWriter(Paths.get("ipgoogle_"+today.getDayOfMonth()+"_"+today.getMonth()+"_"+today.getYear()+".csv"));
            CSVPrinter csvPrinter = new CSVPrinter(writer,
                    CSVFormat.DEFAULT.withHeader("ip", "city_name", "region_name"));

            //Writing IP in the generated CSV file
            hm.forEach((key,value)->{
                String[] s = value.split(":");
                if (s[0]!="-") {
                    try {
                        csvPrinter.printRecord(key, s[0], s[1]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            csvPrinter.flush();
            System.out.println("Write csv file by using new Apache lib successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /* kiem tra ip thuoc private ip va city ko hop le */

    private static boolean checkIpRequirement(String ip, String city) {
        if (city.split(",").length == 1) return false;
        if (ip.isEmpty()) return false;
        long i = ipToLong(ip);
        boolean req1 = i < 167772160 || i > 184549375;
        boolean req2 = i < 2886729728L || i > 2887778303L;
        boolean req3 = i < 3232235520L || i > 3232301055L;
        return req1 && req2 && req3;
    }

    /* chuyen ip sang dang Long */

    private static long ipToLong(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");
        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {
            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);
        }
        return result;
    }
    private static boolean checkDuplicate(String ip, Set<String> ipList) {
        return ipList.contains(ip);
    }
}
