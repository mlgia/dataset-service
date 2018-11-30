package es.accenture.mlgia.service.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import es.accenture.mlgia.service.SparkService;
//TODO Add missing dependency
//import oscuroweb.bigdata.dto.FieldDTO;

@Service
public class SparkServiceImpl implements SparkService {

    @Autowired
    private SparkSession spark;

    @Value("${dataset.file}")
    private String csvFile;
    
    @Value("${dataset.save.file}")
    private String csvSavedFile;
 
    Dataset<Row> parkingDataset;
    
    //TODO Add missing dependency
    //List<FieldDTO> fields;
    
    /**
     * Scheduler Service that construct a Machine Learning model to predict the income.
     */
    @Scheduled(fixedDelay = 86400000)
    @Override
    public void etlService() {
    	
        spark.sparkContext().setLogLevel("ERROR");

        parkingDataset = getDataset();
        
        parkingDataset = parkingDataset
        		.withColumn("output", parkingDataset.col("_c11").gt(0).cast(DataTypes.IntegerType));
        		
		parkingDataset.createOrReplaceTempView("parking");
		
		// Monday => 	0 - working day
		// Tuesday => 	1 - working day
		// Wednesday => 	2 -	working day
		// Thursday => 	3 - working day
		// Friday => 	4 - working day 
		// Saturday => 	5 - weekend
		// Sunday => 	6 - weekend
		parkingDataset = spark.sql(
				"SELECT *, IF(DATE_FORMAT(_c10, 'E') = 'Mon', 0, "
				+ "IF(DATE_FORMAT(_c10, 'E') = 'Tue', 1,"
				+ "IF(DATE_FORMAT(_c10, 'E') = 'Wed', 2,"
				+ "IF(DATE_FORMAT(_c10, 'E') = 'Thu', 3,"
				+ "IF(DATE_FORMAT(_c10, 'E') = 'Fri', 4,"
				+ "IF(DATE_FORMAT(_c10, 'E') = 'Sat', 5,"
				+ "IF(DATE_FORMAT(_c10, 'E') = 'Sun', 6, -1))))))) as day_of_week,"
				+ "DATE_FORMAT(_c10, 'H') as hour_of_day "
				+ "FROM parking ");
		
		// 7 - 10 	=> 0
		// 10 - 13  => 1
		// 13 - 16 	=> 2
		// 16 - 19 	=> 3
		// 19 - 22 	=> 4
		// 22 - 0 	=> 5
		// 0 - 7 => 6
		parkingDataset.createOrReplaceTempView("parking");
		parkingDataset = spark.sql("SELECT *, "
						+ "IF (hour_of_day < 7, 6, "
						+ "IF (hour_of_day >= 7 AND hour_of_day < 10, 0, "
						+ "IF (hour_of_day >= 10 AND hour_of_day < 13, 1, "
						+ "IF (hour_of_day >= 13 AND hour_of_day < 16, 2, "
						+ "IF (hour_of_day >= 16 AND hour_of_day < 19, 3, "
						+ "IF (hour_of_day >= 19 AND hour_of_day < 22, 4, "
						+ "IF (hour_of_day >= 22, 5, "
						+ "-1))))))) as time_zone "
						+ "FROM parking ");
	
        parkingDataset = parkingDataset
        		.withColumn("working_day", parkingDataset.col("day_of_week").leq(4).cast(DataTypes.IntegerType));
        
        parkingDataset = parkingDataset.filter(parkingDataset.col("_c0").notEqual("null"))
        		.filter(parkingDataset.col("day_of_week").notEqual(-1))
        		.filter(parkingDataset.col("time_zone").notEqual(-1))
        		.select(parkingDataset.col("output"),
        				parkingDataset.col("_c0"),
        				parkingDataset.col("time_zone"),
        				parkingDataset.col("day_of_week"),
        				parkingDataset.col("working_day"));
        
        	parkingDataset.show();
        	
        	parkingDataset.coalesce(1)
        		.write()
        		.option("header", false)
        		.mode(SaveMode.Overwrite)
        		.csv(csvSavedFile);      
    }

    private Dataset<Row> getDataset() {

        Dataset<Row> dataset = spark.read()
        		.option("ignoreLeadingWhiteSpace", true)
        		.csv(csvFile);

        dataset.show();

        return dataset;

    }

}
