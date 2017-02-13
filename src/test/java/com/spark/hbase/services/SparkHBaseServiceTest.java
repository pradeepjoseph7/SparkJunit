package com.spark.hbase.services;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.DataFrame;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.test.base.AbstractTest;
import com.test.constants.TestConstants;
import com.test.util.TestUtil;

/*
 * Test class for testing the HBase insertion code.
 */
public class SparkHBaseServiceTest extends AbstractTest {
	
	private static final Log LOG = LogFactory.getLog(SparkHBaseServiceTest.class);
	
	@BeforeClass
	public static void loadTestData() {
		TestUtil.loadTestData(TestConstants.SPARK_HABSE_TEST_XML.getValue());
	}
	
	@Test
	public void testInsertDataToHBase() {
		try {
			//creating the table for test case execution
			createHBaseTable(TestConstants.DEFAULT_NAMESPACE.getValue(), TestConstants.TEST_TABLE_NAME.getValue(),
					"CF");
			Configuration configuration = hbaseUtil.getConfiguration();
			
			JavaHBaseContext hbaseContext = new JavaHBaseContext(sparkContext, configuration);

			//Fetching the schema of test data for inserting into the table 
			String schema = TestUtil.getSchema(TestConstants.TEST_INSERT_DATA_TO_HBASE.getValue(),
					TestConstants.TEST_TABLE_NAME.getValue());
			//Fetching the test data for inserting into the table
			List<String> testData = TestUtil.getTestData(TestConstants.TEST_INSERT_DATA_TO_HBASE.getValue(),
					TestConstants.TEST_TABLE_NAME.getValue());
			DataFrame df = cretaeDataFrame(testData, schema, sqlContext);
			//Invoking the test method.
			SparkHBaseService.insertDataToHBase(TestConstants.TEST_TABLE_NAME.getValue(),
					df.javaRDD(), hbaseContext);
			//Fetching the inserted test data.
			Result result = getTableData(TestConstants.DEFAULT_NAMESPACE.getValue(), configuration,TestConstants.TEST_TABLE_NAME.getValue(), "Hary");
			//Fetching the expected value to be asserted from xml.
			List<String> expected = TestUtil.getExpectedValue(TestConstants.TEST_INSERT_DATA_TO_HBASE.getValue());
			Assert.assertEquals(Bytes.toString(result.getRow()), expected.get(0));
			//Deleting the test table
			deleteHBaseTable(TestConstants.DEFAULT_NAMESPACE.getValue(), TestConstants.TEST_TABLE_NAME.getValue());
		} catch (IOException e) {
			LOG.info(" - exception occured while testing the testInsertDataToHBase method");
			throw new RuntimeException(e);
		}
	}

}
