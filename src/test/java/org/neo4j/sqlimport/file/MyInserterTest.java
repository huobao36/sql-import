package org.neo4j.sqlimport.file;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neo4j.sqlimport.Field;
import com.neo4j.sqlimport.ForeignKeyInstruction;
import com.neo4j.sqlimport.LongField;
import com.neo4j.sqlimport.SQLImporter;
import com.neo4j.sqlimport.StringField;
import com.neo4j.sqlimport.TableImportInstruction;

public class MyInserterTest {
	
	private static final String SQL_USER = "/home/louis/experiment/xueqiu-dmp/User.sql";
	private static final String SQL_STOCK = "/home/louis/experiment/xueqiu-dmp/Stocks.sql";
	private static final String SQL_USERRELATION = "/home/louis/experiment/xueqiu-dmp/Relations.sql";
	private static final String SQL_USERSTOCK_RELATION = "/home/louis/experiment/xueqiu-dmp/user_stock_rel.sql";
	private static final Field  FIELD_ID = new LongField("id");
	private static final Field  FILED_TARGET_ID = new LongField("targetId");
	private static final Field  FIELD_FOLLOWINGTIME = new LongField("time");
	private static final Field  FIELD_SYMBOL = new StringField("symbol");
	
	private SQLImporter importer;	
	
	@Before
	public void setUp()
	{
		importer = new SQLImporter( "target/xueqiu" );
		importer.deleteDB();
	}

	@Test 
	public void importAll()
	{
		importUsers();
		shutDown();
		importUserRelations();
		shutDown();
		importStocks();
		shutDown();
		importUserStockRel();
	}
	
	@Test
	public void importUsers()
	{
		Field[] fields = { FIELD_ID };
	    Map<Field, String> indexes = new HashMap<Field, String>();
	    indexes.put( FIELD_ID, "uid");
	    TableImportInstruction instruction = new TableImportInstruction( "USERS",
	              "`User`", fields, indexes );
	    importer.addImportInstruction( instruction );
		importer.startImportMultiLines( SQL_USER );
	}
	
	@Test 
	public void importUserRelations()
    {
        Field[] fields = { FIELD_ID, FILED_TARGET_ID, null, null, FIELD_FOLLOWINGTIME};
        importer.addImportInstruction( new ForeignKeyInstruction( fields, "`Relations`",
        		FIELD_ID, "uid", FILED_TARGET_ID, "uid", null, "id",
                TestRelationships.FOLLOWUSER) );
        importer.startImportMultiLines(SQL_USERRELATION);
    }

	@Test 
	public void importStocks()
    {
		Field[] fields = { FIELD_SYMBOL };
	    Map<Field, String> indexes = new HashMap<Field, String>();
	    indexes.put( FIELD_SYMBOL, "symbol");
	    TableImportInstruction instruction = new TableImportInstruction( "STOCKS",
	              "`stocks`", fields, indexes );
	    importer.addImportInstruction( instruction );
		importer.startImportMultiLines( SQL_STOCK );
    }
	
	@After
	public void shutDown() 
	{
		importer.shutdown();
	}
	
	@Test 
	public void importUserStockRel() 
	{
        Field[] fields = { FIELD_ID, FIELD_SYMBOL, FIELD_FOLLOWINGTIME };
        importer.addImportInstruction( new ForeignKeyInstruction( fields, "`user_stock_rel`",
        		FIELD_ID, "uid", FIELD_SYMBOL, "symbol", "id", null,
                TestRelationships.FOLLOWSTOCK) );
        importer.startImportMultiLines(SQL_USERSTOCK_RELATION);
		
	}
}
