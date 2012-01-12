package com.neo4j.sqlimport;

import java.util.HashMap;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;

public class ForeignKeyInstruction extends ImportInstruction
{

    private final Field fromIdField;
    private final String toIndexName;
    private final RelationshipType relationshipType;
    private final String fromIndexName;
    private final Field toIdField;
    private final String fromIdKey;			// search key
    private final String toIdKey;			// search key;

    public ForeignKeyInstruction( Field[] names, String tableName,
            Field fromIdField, String fromIndexName, Field toIdField,
            String toIdIndexName, String fromIdKey, String toIdKey, RelationshipType relationshipType )
    {
        super( names, "INSERT INTO " + tableName + " VALUES" );
        this.fromIndexName = fromIndexName;
        this.toIdField = toIdField;
        this.fromIdField = fromIdField;
        this.toIndexName = toIdIndexName;
        this.relationshipType = relationshipType;
        if(fromIdKey == null && this.fromIdField != null)
        	this.fromIdKey = this.fromIdField.name;
        else
        	this.fromIdKey = fromIdKey;

        if(toIdKey == null && this.toIdField != null)
        	this.toIdKey = this.toIdField.name;
        else
        	this.toIdKey = toIdKey;
    }

    @Override
    public void createData( BatchInserter neo,
            BatchInserterIndexProvider indexProvider,
            HashMap<String, Object> values)
    {
    	BatchInserterIndex fromindex = indexProvider.nodeIndex( fromIndexName,
                MapUtil.stringMap( "type", "exact" ) );
    	Object fromVal = values.get( fromIdField.name ) ;
    	long fromNodeId = -1;
    	try {
    		fromNodeId = fromindex.get( fromIdKey, fromVal).getSingle();
    	} catch (Exception e) {
    		System.out.println(fromVal + " not found;");
    	}
    	BatchInserterIndex toindex = indexProvider.nodeIndex( toIndexName,
                MapUtil.stringMap( "type", "exact" ) );
    	Object toVal = values.get( toIdField.name);
    	long toNodeId = -1;
    	try {
    		toNodeId = toindex.get( toIdKey, toVal).getSingle();
    	} catch (Exception e) {
    		System.out.println(toVal + " not found;");
    	}
        values.remove( fromIdField.name );
        values.remove( toIdField.name );
        neo.createRelationship(fromNodeId, toNodeId, relationshipType, values );
    }
}
