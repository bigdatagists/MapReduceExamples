package com.patientanalytics.serde;
 
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
 
@SuppressWarnings("deprecation")
public class PatientSerde implements SerDe {
       private StructTypeInfo rowTypeInfo;
       private ObjectInspector rowOI;
       private List<String> colNames;
      
       @Override
       public void initialize(Configuration conf, Properties tbl) throws SerDeException {
              String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
              colNames = Arrays.asList(colNamesStr.split(","));
 
              String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
              List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
              rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
              rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
       }
 
       @Override
       public Object deserialize(Writable blob) throws SerDeException {
              return addRowValues(createFieldsMap(blob));
       }
 
       private List<Object> addRowValues(Map<String, Object> root) {
              Object field = null;
              List<Object> row = new ArrayList<Object>();
              for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
                     try {
                           field = root.get(fieldName);
                           if (null != field) {
                                  TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
                                  field = convertToType(field, fieldTypeInfo);
                           }
                     } catch (Exception e) {
                           field = null;
                     }
                     row.add(field);
              }
              return row;
       }
 
       private Object convertToType(Object field, TypeInfo fieldTypeInfo) {
              if (fieldTypeInfo.getQualifiedName().equals("date"))
                     field = Date.valueOf(field.toString());
              else if (fieldTypeInfo.getQualifiedName().equals("int"))
                     field = Integer.valueOf(field.toString());
              else if (fieldTypeInfo.getQualifiedName().equals("timestamp"))
                     field = Timestamp.valueOf(field.toString());
              else if (fieldTypeInfo.getQualifiedName().equals("double"))
                     field = Double.valueOf(field.toString());
              return field;
       }
 
       private Map<String, Object> createFieldsMap(Writable blob) {
              Text blobText = (Text) blob;
              StringTokenizer stringTokenizer = new StringTokenizer(blobText.toString(), "\t");
              String prevToken = null;
              Map<String, Object> root =  new HashMap<String, Object>();
              while (stringTokenizer.hasMoreTokens()) {
                     String token = stringTokenizer.nextToken();
                     String[] tokenArray = token.split("=");
                     if (tokenArray.length == 2) {
                           prevToken = tokenArray[0];
                           root.put(tokenArray[0].toLowerCase(), tokenArray[1]);
                     } else if (tokenArray.length < 2){
                           String value = root.get(prevToken) + " " + token;
                           root.put(prevToken, value);
                     }
              }
              return root;
       }
 
       @Override
       public ObjectInspector getObjectInspector() throws SerDeException {
              return rowOI;
       }
 
       @Override
       public SerDeStats getSerDeStats() {
              return null;
       }
 
       @Override
       public Class<? extends Writable> getSerializedClass() {
              return Text.class;
       }
 
       @Override
       public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException {
              try {
                     return new Text(obj.toString());
              } catch (Exception e) {
                     throw new SerDeException(e);
              }
       }
}