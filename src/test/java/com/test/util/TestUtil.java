package com.test.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.collections.CollectionUtils;

import com.test.constants.TestConstants;
import com.test.jaxb.beans.TestDataSet;
import com.test.jaxb.beans.TestDataSet.Method;
import com.test.jaxb.beans.TestDataSet.Method.Expected;
import com.test.jaxb.beans.TestDataSet.Method.Input;
import com.test.jaxb.beans.TestDataSet.Method.Table;

/*
 * Util class for loading the test data from xml for testcases.
 */
public class TestUtil {

	private static Map<String, Map<String, Map<String, Object>>> dataMap = new HashMap<>();

	/*
	 * The method to load the test data.
	 */
	public static void loadTestData(String fileName) {
		dataMap = getParseData(fileName);
	}

	/*
	 * The method to parse the test xml.
	 */
	public static Map<String, Map<String, Map<String, Object>>> getParseData(String fileName) {
		Map<String, Map<String, Map<String, Object>>> dataMap = new HashMap<>();
		TestDataSet testDataSet = loadXML(fileName);
		if (null != testDataSet && CollectionUtils.isNotEmpty(testDataSet.getMethod())) {
			for (Method method : testDataSet.getMethod()) {
				if (null == dataMap.get(method.getName())) {
					dataMap.put(method.getName(), new HashMap<String, Map<String, Object>>());
				}
				Map<String, Map<String, Object>> methodMap = dataMap.get(method.getName());
				for (Table tableData : method.getTable()) {
					if (null == methodMap.get(tableData.getName())) {
						methodMap.put(tableData.getName(), new HashMap<String, Object>());
					}
					Map<String, Object> tableMap = methodMap.get(tableData.getName());
					tableMap.put(TestConstants.SCHEMA.getValue(), tableData.getColumn());
					for (String rowData : tableData.getRow()) {
						if (null == tableMap.get(TestConstants.ROWS.getValue())) {
							tableMap.put(TestConstants.ROWS.getValue(), new ArrayList<>());
						}
						List<String> rows = (List<String>) tableMap.get(TestConstants.ROWS.getValue());
						rows.add(rowData);
						tableMap.put(TestConstants.ROWS.getValue(), rows);
					}
					methodMap.put(tableData.getName(), tableMap);
				}
				Expected expected = method.getExpected();
				if (null != expected) {
					Map<String, Object> valueMap = new HashMap<String, Object>();
					List<String> valueList = new ArrayList<>();
					if (CollectionUtils.isNotEmpty(expected.getValue())) {
						for (String value : expected.getValue()) {
							valueList.add(value);
						}
					}
					valueMap.put(TestConstants.EXPECTED.getValue(), valueList);
					methodMap.put(TestConstants.EXPECTED.getValue(), valueMap);
				}
				Input input = method.getInput();
				if (null != input) {
					Map<String, Object> valueMap = new HashMap<String, Object>();
					List<String> valueList = new ArrayList<>();
					if (CollectionUtils.isNotEmpty(input.getValue())) {
						for (String value : input.getValue()) {
							valueList.add(value);
						}
					}
					valueMap.put(TestConstants.INPUT.getValue(), valueList);
					methodMap.put(TestConstants.INPUT.getValue(), valueMap);
				}
				dataMap.put(method.getName(), methodMap);
			}
		}
		return dataMap;
	}

	/*
	 * This method will load the test xml using jaxb classes.
	 */
	public static TestDataSet loadXML(String fileName) {
		TestDataSet testDataSet = null;
		try {
			InputStream input = TestUtil.class.getClassLoader().getResourceAsStream(fileName);
			JAXBContext jaxbContext = JAXBContext.newInstance(TestDataSet.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			testDataSet = (TestDataSet) jaxbUnmarshaller.unmarshal(input);
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return testDataSet;
	}

	/*
	 * This method will load the test data for a particular table from xml.
	 */
	public static List<String> getTestData(String methodName, String tableName) {
		List<String> dataList = new ArrayList<>();
		Map<String, Map<String, Object>> methodMap = dataMap.get(methodName);
		if (null != methodMap) {
			Map<String, Object> tableMap = methodMap.get(tableName);
			dataList = (List<String>) tableMap.get(TestConstants.ROWS.getValue());
		}
		return dataList;
	}

	/*
	 * This method will load the test schema for a particular table from xml.
	 */
	public static String getSchema(String methodName, String tableName) {
		String schema = null;
		Map<String, Map<String, Object>> methodMap = dataMap.get(methodName);
		if (null != methodMap) {
			Map<String, Object> tableMap = methodMap.get(tableName);
			schema = (String) tableMap.get(TestConstants.SCHEMA.getValue());
		}
		return schema;
	}

	/*
	 * This method will load the expected value for a particular test method
	 * from xml.
	 */
	public static List<String> getExpectedValue(String methodName) {
		List<String> expectedValue = null;
		Map<String, Map<String, Object>> methodMap = dataMap.get(methodName);
		if (null != methodMap) {
			Map<String, Object> expectedMap = methodMap.get(TestConstants.EXPECTED.getValue());
			expectedValue = (List<String>) expectedMap.get(TestConstants.EXPECTED.getValue());
		}
		return expectedValue;
	}

	/*
	 * This method will load the test data for a particular test method from
	 * xml.
	 */
	public static List<String> getInputValue(String methodName) {
		List<String> inputValue = null;
		Map<String, Map<String, Object>> methodMap = dataMap.get(methodName);
		if (null != methodMap) {
			Map<String, Object> inputMap = methodMap.get(TestConstants.INPUT.getValue());
			inputValue = (List<String>) inputMap.get(TestConstants.INPUT.getValue());
		}
		return inputValue;
	}

}