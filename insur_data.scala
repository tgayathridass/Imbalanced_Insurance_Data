import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,sum}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank

import org.apache.spark.sql.functions._

// Building a model to predict customer would be interested in Vehicle Insurance 
// 0: 319594
// 1: 62531
// 
object insur2 extends App{
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .appName("myApp1")
  .master("local[2]")
  .enableHiveSupport()
  .getOrCreate()
  
  //Data Reading
  val inputfile = spark.read.format("csv")
                  .option("header",true)
                  .option("inferSchema",true)
                  .option("path","C:/DATA/Documents/test_data.csv")
                  .load
                                             
               val total_rows = inputfile.count()
               val total_columns = inputfile.columns.length
   println("total no of columns: "+total_columns)
   println("total no of rows: "+total_rows)
                  
                 // Map passes each element of the source through a function and forms a new distributed dataset.
              // Spark RDD reduce () aggregate action function is used to calculate min, max, and total of elements in a dataset
  
  //1.Data validation: null check           
    val result = inputfile.select( 
                            inputfile.columns
                           .map(x => sum( isnull(col(x))cast "int" ) )
                           .reduce(_+_) as "total_nulls"
                           )   
	    result.show
               
                      
  //2. Data pre-processing  : remove unwanted columns
		
   val drpCols = Seq("Gender","Vehicle_Age","Vehicle_Damage")
   val numdf = inputfile.drop(drpCols:_*) 
 
   numdf.createOrReplaceTempView("num_data") 
   val numStatAnlse = spark.sql("select *from num_data")
  
   //3. latest data check: crosscheck if the source data is already present in target data  --check null counts      
   val latDtata = spark.sql("select count(id) from num_data where id not in (select id from hive_tab)")
   if(latData = 0 )
	{ println("Data already exists") }
   else  
	{
   println("--------DUPLICATE ID COUNTS--------")        
   spark.sql("select id , count(id) from num_data group by id having count(id) > 1").show()
       
   println("---------CUSTOMERS RESPONSE COUNT-------")  
  // numStatAnlse.show()
  spark.sql("select response ,count(1)as No_Of_Customers from num_data group by response ").show()
  
  println("-------------SUCESS_RATE---------------------")  
  val subscribed = numStatAnlse.filter("response == '1'").count().toDouble
  val totalcount = numStatAnlse.count().toDouble  
  val non_subscribed = numStatAnlse.filter("response == '0'").count().toDouble
  val success_rate = subscribed/totalcount
  val fail_rate = non_subscribed/totalcount
  
  println("Success Rate :"+success_rate) 
  println("Failure Rate :"+fail_rate) 

  println("---------MIN,MAX,AVG AGE'S_OF_EXISTING_CUSTOMERS-------")  
  spark.sql("select min(Age), avg(Age), max(Age) from num_data").show()

   
  println("---------COUNT_OF_PEOPLE_BY_AGE_WHO_PURCHASED_POLICY-------")  
  spark.sql("Select age, count(*) as totalCount from num_data where response=1 group by age").show()
  
  println("---------premiuim customers---")
  val premCust = spark.sql("select id, Annual_Premium from num_data where Previously_Insured=1 and Response=1 order by Annual_Premium desc ") 
  premCust.show()
  

  premCust.write.mode("overwrite").saveAsTable("test_db.prem_customers_details")
  
  
  //Data Enrichment:
  println("---------FEATURE_ENGINEERING_RIGHT_AGE_FOR_POLICY -------")  
  spark.sql("""select 
          case when Age<25 then 'Young' 
               when Age between 25 and 60 then 'Middle_Age' 
               when Age>=60 then 'Old' 
           end as Age_Category, count(*) 
         from num_data 
         where response=1 
         group by Age_Category 
         order by Age_Category desc""").show()
  
  
         
  println("---------MAX SOLD BY CHANNELS-------")  
  spark.sql("select  Policy_Sales_Channel, response from num_data")
 .groupBy("Policy_Sales_Channel")
 .pivot("response")
 .count()
 .show(50)  
   
  println("---------SUBSCRIPTION COUNT OF PREV INSURED CUSTOMERS---------")
  spark.sql("select  Previously_Insured, response from num_data")
 .groupBy("Previously_Insured")
 .pivot("response")
 .count()
 .show(50)
 
  println("")    
       
 println("---------TOP 10 Sales Channels-------")    
 val agentdf = inputfile.filter("Response == 1")
              .groupBy("Policy_Sales_Channel")
              .count()
              .sort(col("count").desc)
              .withColumn("ranking",dense_rank().over(Window.orderBy(col("count").desc)))
              .show(10)
                   
                        
 println("--------------------RESPONSE BASED ON VEHICLE AGE------------ ")                
 val vehage =  inputfile.createOrReplaceTempView("temp_data")
 spark.sql("select Vehicle_Age, count(*) from temp_data where Response = 1 group by Vehicle_Age").show
                 
	}          
 spark.stop()
               
             
                  
  
  
}
