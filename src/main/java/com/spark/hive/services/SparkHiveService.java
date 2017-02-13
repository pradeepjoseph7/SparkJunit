package com.spark.hive.services;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Class for querying data from Hive.
 */
public class SparkHiveService {

	public static final Logger LOGGER = LoggerFactory.getLogger(SparkHiveService.class);

	/*
	 * The method for querying data from Hive.
	 */
	public static JavaRDD<Row> loadHiveData(SQLContext hiveCtx) throws Exception {
		try {

			// Query for fetching data from employee table
			String employeeQuery = "SELECT employeename as employee_name, age as employee_age from employee where age > '30'";
			DataFrame employeeDataFrame = hiveCtx.sql(employeeQuery);

			// Query for fetching data from address table
			String addressQuery = "SELECT employeename as employee_address_name, streetname as street_name from address";
			DataFrame addressDataFrame = hiveCtx.sql(addressQuery);

			// Query for fetching data from role table
			String roleQuery = "SELECT employeename as employee_role_name, rolename as rolename from role where rolename = 'admin'";
			DataFrame roleDataFrame = hiveCtx.sql(roleQuery);
			DataFrame employeeAndAddDF = employeeDataFrame.join(addressDataFrame,
					employeeDataFrame.col("employee_name").equalTo(addressDataFrame.col("employee_address_name")),
					"left_outer");
			DataFrame aggregatedDF = employeeAndAddDF.join(roleDataFrame,
					employeeAndAddDF.col("employee_name").equalTo(roleDataFrame.col("employee_role_name")),
					"left_outer");

			// filtering the resultant RDD to avoid the records with employee
			// name as null
			JavaRDD<Row> fiteredRDD = aggregatedDF.javaRDD().filter(new Function<Row, Boolean>() {
				private static final long serialVersionUID = -3642286645364921749L;

				@Override
				public Boolean call(Row row) throws Exception {
					String rowKey = row.getAs("employee_name");
					return StringUtils.isNotBlank(rowKey);
				}
			});
			return fiteredRDD;
		} catch (Exception exception) {
			LOGGER.error("Error", exception);
			throw exception;
		}
	}

}
