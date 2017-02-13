package com.spark.hbase.services;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/*
 * Class for Inserting data into HBase.
 */
public class SparkHBaseService {

	/*
	 * The method for inserting data into HBase
	 */
	public static void insertDataToHBase(String tableName, JavaRDD<Row> fiteredRDD, JavaHBaseContext hbaseContext) {
		hbaseContext.bulkPut(fiteredRDD, TableName.valueOf(tableName), new Function<Row, Put>() {
			private static final long serialVersionUID = 1L;

			// Create put object for each row
			public Put call(Row row) throws Exception {
				String rowKey = row.getAs("student_name");
				Put put = new Put(Bytes.toBytes(rowKey));
				put.add(CellUtil.createCell(Bytes.toBytes(rowKey), Bytes.toBytes("CF"),
						Bytes.toBytes("student_organization".toUpperCase()), System.currentTimeMillis(),
						KeyValue.Type.Minimum.getCode(), Bytes.toBytes(row.getAs("student_organization").toString())));
				put.add(CellUtil.createCell(Bytes.toBytes(rowKey), Bytes.toBytes("CF"),
						Bytes.toBytes("student_type".toUpperCase()), System.currentTimeMillis(),
						KeyValue.Type.Minimum.getCode(), Bytes.toBytes(row.getAs("student_type").toString())));
				return put;
			}
		});
	}
}
