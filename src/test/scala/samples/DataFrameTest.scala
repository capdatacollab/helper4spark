package samples



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._




@Test
class DataFrameTest {

    /**
     * Class to simulate DataFrame nested schema
     */
    case class TestA(name: String, date_obj: Int, type_obj:String)
    case class TestB(obj_testA1:TestA, obj_testA2:TestA)
  
    
    /**
     * Create DataSet for test
     */
    def getDataFrame(sc:SparkContext, sqlContext:SQLContext): DataFrame={
      import sqlContext.implicits._
      return sc.parallelize(
            TestB(TestA("a",3, "C"),TestA("a",3, ""))
          ::TestB(TestA("a",4, "M"),TestA("a",4, ""))
          ::TestB(TestA("a",5, "S"),TestA("a",5, "")) 
          ::TestB(TestA("b",5, "M"),TestA("b",5, ""))  
          ::TestB(TestA("b",4, "M"),TestA("b",4, ""))   
          ::TestB(TestA("b",3, "C"),TestA("b",3, ""))    
          ::Nil ).toDF()
    }
    
    
    @Test
    def queryTest() = {
      // Initialize Spark Context
       val conf = new SparkConf()
         .setMaster("local")
         .setAppName("DataFrameTest")
         .registerKryoClasses(Array(classOf[TestA], classOf[TestB]));
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc) // SQLContextSingleton.getInstance(sc)
      
      
      
      
      // Create an object with 2 levels : (dataTest.printSchema)
      //root
      // |-- obj_testA1: struct (nullable = true)
      // |    |-- name: string (nullable = true)
      // |    |-- date: integer (nullable = false)
      // |    |-- type_obj: string (nullable = true)
      // |-- obj_testA2: struct (nullable = true)
      // |    |-- name: string (nullable = true)
      // |    |-- date: integer (nullable = false)
      // |    |-- type_obj: string (nullable = true)
      //      val test = getDataFrame(sc, sqlContext)
      val dataTest = getDataFrame(sc, sqlContext);

      
      
      //////////////////////////////////
      // Method 1, SQL style
      //////////////////////////////////
      
      // Register dataFrame as table
      dataTest.registerTempTable("tmp_dataTest")   
      
      // Query 
      val result_1 = sqlContext.sql(""" 
          SELECT 
                obj_testA1
              , obj_testA2
          FROM
          (
            SELECT 
                  first(A.obj_testA1) AS obj_testA1
                , first(A.obj_testA2) AS obj_testA2
            FROM (
              SELECT
                  *
                FROM tmp_dataTest
                WHERE obj_testA2.date_obj <= 3
                ORDER BY obj_testA1.date_obj DESC
            ) A
            GROUP BY A.obj_testA2.name 
          ) B
          Where B.obj_testA1.type_obj!='S'
        """)
        .toJSON
        .collect()
        .mkString("\n")
      
      
      //////////////////////////////////
      // Method 2, DataFrame API 
      //////////////////////////////////
      val result_2 =  dataTest
        .filter("obj_testA2.date_obj <= 3")
        .orderBy(desc("obj_testA1.date_obj"))
        .groupBy("obj_testA2.name")
        .agg(
                first("obj_testA1").as("obj_testA1")
              , first("obj_testA2").as("obj_testA2")
         )
        .filter("obj_testA1.type_obj!='S'")
        .select("obj_testA1", "obj_testA2")
        .toJSON
        .collect()
        .mkString("\n")
        
       
        
      
      
      // Close SparkContext
      sc.stop();    
      
       
      // Test result
      assertEquals(result_1, result_2)
      
      // Result :
      // {
      //   "obj_testA1":{"name":"a","date_obj":3,"type_obj":"C"}
      //  ,"obj_testA2":{"name":"a","date_obj":3,"type_obj":""}
      // }
      // {
      //  "obj_testA1":{"name":"b","date_obj":3,"type_obj":"C"}
      //  ,"obj_testA2":{"name":"b","date_obj":3,"type_obj":""}
      // }
      
      
    }



}


