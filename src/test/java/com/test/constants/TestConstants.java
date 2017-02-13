package com.test.constants;

public enum TestConstants {
	 /** The Constant for testLoadHiveData method. */
    TEST_LOAD_HIVE_DATA("testLoadHiveData"),
    
    /** The Constant for employee table name. */
    EMPLOYEE("employee"),
    
    /** The Constant for address table name. */
    ADDRESS("address"),
    
    /** The Constant for role table name. */
    ROLE("role"),
    
    /** The Constant for DEFAULT. */
    DEFAULT_NAMESPACE("default"),
    
    /** The Constant for testInsertDataToHBase method. */
    TEST_INSERT_DATA_TO_HBASE("testInsertDataToHBase"),
    
    /** The Constant for SPARK_HABSE_TEST_XML XML. */
    SPARK_HABSE_TEST_XML("xml/hbasetest.xml"),
    
    /** The Constant for SPARK_HIVE_TEST_XML XML. */
    SPARK_HIVE_TEST_XML("xml/hivetest.xml"),
    
    /** The Constant for TEST_TABLE_NAME XML. */
    TEST_TABLE_NAME("TEST_TABLE"),
    
    /** The Constant for SCHEMA. */
    SCHEMA("SCHEMA"),
    
    /** The Constant for ROWS. */
    ROWS("ROWS"),
    
    /** The Constant for EXPECTED. */
    EXPECTED("EXPECTED"),
    
    /** The Constant for INPUT. */
    INPUT("INPUT");
    
    /** The value. */
    private String value;

    /**
     * Instantiates a new process status type.
     * 
     * @param value
     *            the value
     */
    private TestConstants(String value) {

        this.value = value;
    }

    /**
     * Gets the value.
     * 
     * @return the value
     */
    public String getValue() {

        return value;
    }
    
}
