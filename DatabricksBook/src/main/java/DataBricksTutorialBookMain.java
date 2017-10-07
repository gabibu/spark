import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.window;

public class DataBricksTutorialBookMain
{
    final static Logger logger = Logger.getLogger(DataBricksTutorialBookMain.class);

    public static void main(String[] args)
    {
        try
        {
            //page 31
            //sqlAndDataSet();
            //appleWindow();
            appleStream();
        }
        catch (Exception e)
        {
            logger.error(e);
        }


    }

    private static void appleWindow() throws URISyntaxException
    {
        try
        {
            SparkConf sparkConf = new SparkConf();
            sparkConf.setMaster("local[4]");
            sparkConf.setAppName("AST TEST");
            System.out.println("completed!!!");
            JavaSparkContext sc = new JavaSparkContext(sparkConf);
            SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
            ClassLoader classLoader = DataBricksTutorialBookMain.class .getClassLoader();
            spark.conf().set("spark.sql.shuffle.partitions", "5");

            URL resource = classLoader.getResource("appl/AAPL_P_2017.csv");
            String path = Paths.get(resource.toURI()).toFile().getPath();

            Dataset<Row> stocksDF = spark.read().option("header","true").
                    option("inferSchema","true")
                    .csv(path);

            Dataset<Row> stocks2017 = stocksDF.filter("year(Date)==2017");

            Dataset<Row> tumblingWindowDS = stocks2017
                    .groupBy(window(stocks2017.col("Date"),"1 week"))
                    .agg(avg("Close").as("weekly_average"));

            List<Row> avgWeek = tumblingWindowDS.collectAsList();
            logger.info("done");
        }
        catch (Exception e)
        {
            logger.error(e);
            throw e;
        }

    }


    private static void appleStream() throws URISyntaxException, InterruptedException {
        try
        {
            SparkConf sparkConf = new SparkConf();
            sparkConf.setMaster("local[4]");
            sparkConf.setAppName("AST TEST");
            System.out.println("completed!!!");
            JavaSparkContext sc = new JavaSparkContext(sparkConf);
            SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
            ClassLoader classLoader = DataBricksTutorialBookMain.class .getClassLoader();
            spark.conf().set("spark.sql.shuffle.partitions", "5");

            URL resource = classLoader.getResource("appl");//AAPL_P_2017.csv
            String path = Paths.get(resource.toURI()).toFile().getPath() + "\\*.csv";

            Dataset<Row> stocksDF = spark.read().option("header","true").
                    option("inferSchema","true")
                    .csv(path );


            Dataset<Row> stocksDFStream = spark.readStream()
                    .schema(stocksDF.schema())
                    .option("maxFilesPerTrigger", "1")
                    .option("header","true").
                    option("inferSchema","true")
                    .csv(path);

            logger.info("isStreaming" + stocksDFStream.isStreaming());



            Dataset<Row> tumblingWindowDS = stocksDFStream
                    .groupBy(window(stocksDFStream.col("Date"),"1 week"))
                    .agg(avg("Close").as("week_average"));

            StreamingQuery streamingQuery = tumblingWindowDS.writeStream().format("memory").queryName("appl_week")
                    .outputMode("complete").start();


            Thread.sleep(TimeUnit.SECONDS.toMillis(2));

            long endTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);


            while (System.currentTimeMillis() < endTime)
            {
                List<Row> currentValues
                        = spark.sql(" select * from appl_week").collectAsList();

                logger.info("currentValues => " + currentValues);
            }


            logger.info("done");
        }
        catch (Exception e)
        {
            logger.error(e);
            throw e;
        }

    }


    /*
      play with sql and dataset
     */
    private static void sqlAndDataSet() throws URISyntaxException {
        try
        {
            SparkConf sparkConf = new SparkConf();
            sparkConf.setMaster("local[4]");
            sparkConf.setAppName("AST TEST");
            System.out.println("completed!!!");
            JavaSparkContext sc = new JavaSparkContext(sparkConf);
            SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
            ClassLoader classLoader = DataBricksTutorialBookMain.class .getClassLoader();


            URL resource = classLoader.getResource("flights.csv");
            String path = Paths.get(resource.toURI()).toFile().getPath();

            spark.conf().set("spark.sql.shuffle.partitions", "5");


            Dataset<Row> flightData2015 = spark
                    .read().option("header", "true").
                            csv(path);


            flightData2015.createOrReplaceTempView("flight_data_2015");

            Dataset<Row> sqlOriginAirportCount
                    = spark.sql("select ORIGIN_AIRPORT_ID, count(1)  from flight_data_2015 group by ORIGIN_AIRPORT_ID")
                    .withColumnRenamed("count(1)", "originCount").orderBy("originCount");



            Dataset<Row> dataframeOriginAirportCount
                    =  flightData2015.groupBy("ORIGIN_AIRPORT_ID").count().withColumnRenamed("count",
                     "originCount").sort("originCount");

            List<Row> sqlRes = sqlOriginAirportCount.collectAsList();
            List<Row> dataSetRes  = dataframeOriginAirportCount.collectAsList();


            System.out.println(flightData2015.count());
            System.out.println("dsadsadsa");
        }
        catch (Exception e)
        {
            logger.error(e);
            throw e;
        }

    }
}
