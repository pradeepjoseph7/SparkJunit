package com.spark.hive.services;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.spark.hbase.services.SparkHBaseServiceTest;
import com.test.base.AbstractTest;
import com.test.constants.TestConstants;
import com.test.util.TestUtil;

/*
 * Test class for testing the Hive retrieval code.
 */
public class SparkHiveServiceTest extends AbstractTest {

private static final Log LOG = LogFactory.getLog(SparkHBaseServiceTest.class);
	
	@BeforeClass
	public static void loadTestData() {
		TestUtil.loadTestData(TestConstants.SPARK_HIVE_TEST_XML.getValue());
	}
	
	@Test
	public void testLoadHiveData() {
		try {
			//Fetching the schema of test data for inserting into the table 
			String schema = TestUtil.getSchema(TestConstants.TEST_LOAD_HIVE_DATA.getValue(),
					TestConstants.EMPLOYEE.getValue());
			//Fetching the test data for inserting into the table
			List<String> testData = TestUtil.getTestData(TestConstants.TEST_LOAD_HIVE_DATA.getValue(),
					TestConstants.EMPLOYEE.getValue());
			//creating the employee temporary table
			createTempTable(testData, schema, TestConstants.EMPLOYEE.getValue(), sqlContext);

			//Fetching the schema of test data for inserting into the table 
			schema = TestUtil.getSchema(TestConstants.TEST_LOAD_HIVE_DATA.getValue(),
					TestConstants.ADDRESS.getValue());
			//Fetching the test data for inserting into the table
			testData = TestUtil.getTestData(TestConstants.TEST_LOAD_HIVE_DATA.getValue(),
					TestConstants.ADDRESS.getValue());
			//creating the address temporary table
			createTempTable(testData, schema, TestConstants.ADDRESS.getValue(), sqlContext);

			schema = TestUtil.getSchema(TestConstants.TEST_LOAD_HIVE_DATA.getValue(),
					TestConstants.ROLE.getValue());
			testData = TestUtil.getTestData(TestConstants.TEST_LOAD_HIVE_DATA.getValue(),
					TestConstants.ROLE.getValue());
			//creating the role temporary table
			createTempTable(testData, schema, TestConstants.ROLE.getValue(), sqlContext);

			//Invoking the test method.
			JavaRDD<Row> mapViewRDD = SparkHiveService
					.loadHiveData(sqlContext);
			Row row = mapViewRDD.first();
			String employeeName = row.getAs("employee_name");
			String employeAddress = row.getAs("street_name");
			String roleName = row.getAs("rolename");
			String[] actuals = { employeeName, employeAddress, roleName };
			//Fetching the expected value to be asserted from xml.
			List<String> expected = TestUtil.getExpectedValue(TestConstants.TEST_LOAD_HIVE_DATA.getValue());
			String[] expecteds = expected.get(0).split("\\|");
			Assert.assertArrayEquals(expecteds, actuals);
		} catch (Exception e) {
			LOG.info(" - exception occured while testing the testLoadHiveData method" + e);
			throw new RuntimeException(e);
		}
	}

}
