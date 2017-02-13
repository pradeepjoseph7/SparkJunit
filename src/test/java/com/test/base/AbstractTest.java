package com.test.base;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.test.TestHive$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.io.Files;

/*
 * The base class which does the context initialization and provides base methods.
 */
public class AbstractTest {

	private static final String DEFAULT = "default";
	protected static transient JavaSparkContext sparkContext;
	protected static transient HBaseTestingUtility hbaseUtil;

	private static final Log LOG = LogFactory.getLog(AbstractTest.class);
	protected static transient SQLContext sqlContext;
	DataFrame df;

	@BeforeClass
	public static void setUp() throws IOException {
		initializeHiveContext();
		initializeHBaseContext();
	}

	/*
	 * The method to initialize the Hive Context.
	 */
	public static void initializeHiveContext() {
		if (sqlContext == null) {
			sqlContext = TestHive$.MODULE$;
		}
		if (sparkContext == null) {
			sparkContext = new JavaSparkContext(sqlContext.sparkContext());
		}
	}

	/*
	 * The method to destroy the Hive Context.
	 */
	public static void destroyHiveContext() {
		LOG.info("stopping the hive context");
		sqlContext.sparkContext().stop();
		sqlContext = null;
		sparkContext = null;
	}

	/*
	 * The method to initialize the HBase Context.
	 */
	public static void initializeHBaseContext() {
		if (sparkContext == null) {
			sparkContext = new JavaSparkContext("local", "SparkTestSuite");
		}

		File tempDir = Files.createTempDir();
		tempDir.deleteOnExit();

		if (hbaseUtil == null) {
			hbaseUtil = HBaseTestingUtility.createLocalHTU();
		}
		try {
			if (hbaseUtil.getZkCluster() == null) {
				hbaseUtil.cleanupTestDir();
				LOG.info("starting minicluster");
				hbaseUtil.startMiniZKCluster();
			}
			if (hbaseUtil.getMiniHBaseCluster() == null) {
				hbaseUtil.startMiniHBaseCluster(1, 1);
				LOG.info(" - minicluster started");
			}
		} catch (Exception exception) {
			LOG.info(" - Exception occured while starting mini cluster");
			throw new RuntimeException(exception);
		}
	}

	/*
	 * The method to destroy the HBase Context.
	 */
	public static void destroyHBaseContext() {
		try {
			hbaseUtil.shutdownMiniHBaseCluster();
			hbaseUtil.shutdownMiniZKCluster();
			LOG.info(" - minicluster shut down");
			hbaseUtil.cleanupTestDir();
		} catch (Exception e) {
			LOG.info(" - Exception in destroying HBase Context");
		}

	}

	/*
	 * The method to create a HBase table.
	 */
	public void createHBaseTable(String nameSpaceStr, String tableNameStirng, String columnFamilyStr)
			throws IOException {
		byte[] tableName = Bytes.toBytes(tableNameStirng);
		byte[] columnFamily = Bytes.toBytes(columnFamilyStr);
		byte[] nameSpace = Bytes.toBytes(nameSpaceStr);
		try {
			hbaseUtil.deleteTable(TableName.valueOf(nameSpace, tableName));
		} catch (Exception e) {
			LOG.info(" - no table " + nameSpaceStr + ":" + Bytes.toString(tableName) + " found");
		}

		LOG.info(" - creating table " + nameSpaceStr + ":" + Bytes.toString(tableName));
		NamespaceDescriptor[] descriptors = hbaseUtil.getHBaseAdmin().listNamespaceDescriptors();
		boolean flag = false;
		for (NamespaceDescriptor namespaceDescriptor : descriptors) {
			if (namespaceDescriptor.getName().equals(nameSpaceStr)) {
				flag = true;
				break;
			}
		}
		if (!StringUtils.equals(DEFAULT, nameSpaceStr) && !flag) {
			hbaseUtil.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(nameSpaceStr).build());
		}
		hbaseUtil.createTable(TableName.valueOf(nameSpace, tableName), columnFamily);

		LOG.info(" - created table = " + tableName);
	}

	/*
	 * The method to delete the HBase table.
	 */
	public void deleteHBaseTable(String nameSpaceStr, String tableNameStirng) throws IOException {
		byte[] tableName = Bytes.toBytes(tableNameStirng);
		byte[] nameSpace = Bytes.toBytes(nameSpaceStr);
		try {
			hbaseUtil.deleteTable(TableName.valueOf(nameSpace, tableName));
		} catch (Exception e) {
			LOG.info(" - no table " + nameSpace + ":" + Bytes.toString(tableName) + " found");
		}

	}

	/*
	 * The method to retrieve data from the HBase table.
	 */
	public Result getTableData(String nameSpace, Configuration conf, String tableName, String rowKey)
			throws IOException {
		Connection conn;
		try {
			conn = ConnectionFactory.createConnection(conf);
			Table table = conn.getTable(TableName.valueOf(nameSpace, tableName));
			return table.get(new Get(Bytes.toBytes(rowKey)));
		} catch (IOException e) {
			LOG.info(" - Exception in getting record from " + nameSpace + ":" + tableName);
			throw e;
		}
	}

	/*
	 * The method to insert the data into the HBase table.
	 */
	public void insertTable(String nameSpace, Configuration conf, String tableName, String rowKey) throws IOException {
		Connection conn;
		try {
			conn = ConnectionFactory.createConnection(conf);
			Table table = conn.getTable(TableName.valueOf(nameSpace, tableName));
			table.put(new Put(Bytes.toBytes(rowKey)));
		} catch (IOException e) {
			LOG.info(" - Exception in getting record from " + nameSpace + ":" + tableName);
			throw e;
		}
	}

	/*
	 * The method to drop HBase tables.
	 */
	public void dropTables(List<String> hBaseTable, List<String> hiveTables) {

		for (String tableName : hiveTables) {
			if (sqlContext != null) {
				sqlContext.sql("DROP TABLE IF EXISTS " + tableName);
			}
		}
		for (String tableName : hBaseTable) {
			try {
				hbaseUtil.deleteTable(TableName.valueOf(tableName));
			} catch (IOException e) {
				LOG.info(" - exception occured while dropping the table");
				e.printStackTrace();
			}
		}

	}

	/*
	 * The method to create a temp table with the given schema and test data.
	 */
	public void createTempTable(List<String> testData, String schema, String tableName, SQLContext context) {
		DataFrame df = cretaeDataFrame(testData, schema, context);
		df.registerTempTable(tableName);
	}

	/*
	 * The method to create a dataframe with the given schema and test data.
	 */
	public DataFrame cretaeDataFrame(List<String> testData, String schema, SQLContext context) {
		List<Row> rowList = new ArrayList<Row>();
		for (String array : testData) {
			rowList.add(RowFactory.create((Object[]) array.split("\\|")));
		}
		JavaRDD<Row> rdd = sparkContext.parallelize(rowList);
		StructType customSchema = createDataFrameSchema(schema);
		DataFrame df = context.createDataFrame(rdd, customSchema);
		return df;
	}

	/*
	 * The method to create a schema for the dataframe.
	 */
	public static StructType createDataFrameSchema(String columnDetails) {
		List<StructField> fieldsList = new ArrayList<>();
		if (columnDetails != null) {
			String[] data = columnDetails.split("\\|");
			for (String column : data) {
				DataType dType = DataTypes.StringType;
				fieldsList.add(DataTypes.createStructField(column.trim(), dType, true));
			}
		}
		StructType customSchema = DataTypes.createStructType(fieldsList);
		return customSchema;
	}

	@AfterClass
	public static void tearDown() {
		try {
			LOG.info("tear down method");
			// destroyHBaseContext();
			// destroyHiveContext();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
