package edu.buffalo.cse562;
//
//public class Main {
//	public static void main(String[] args) {
//		System.out.println("We, the members of our team, agree that we will not submit any code that we have not written ourselves, share our code with anyone outside of our group, or use code that we have not written ourselves as a reference.");
//		// 4 commit sathish
//		
//	}
//}


import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;

import java.util.ArrayList;
import edu.buffalo.cse562.Datum;

public class Main {
	/* 
	 *   tableMappings : {tableName : [ {ColumnIndex : ColumnType}, { ColumnName : ColumnIndex}]}
	 *   first hashMap (indexDataTypeMap) -  used in Scan Operators too get the data Type given the index number from iterating data files
	 *   second HashMap (nameIndex)  - for future use guess will be needed in Projection , Selection - use  
	 */
	static HashMap<String, ArrayList<HashMap<?,?>>> tableMappings = new HashMap<String, ArrayList<HashMap<?,?>>>();
	 
	public static void main(String[] args) {		
		//the sql file starts from 3rd argument
		if(args.length < 3){
			System.out.println("Incomplete arguments");
			return;
		}
		
		if (args[0].equals("--data")){
			ConfigManager.setDataDir(args[1]);
		}
			
		for(int i=2; i < args.length; i++){				
			Path sqlFile = FileSystems.getDefault().getPath(args[i]);		
			Charset charset = Charset.forName("US-ASCII");				
			Statement statement;								
			
			try (BufferedReader reader = Files.newBufferedReader(sqlFile, charset)){
				String line = null;
				while ((line = reader.readLine()) != null){
					CCJSqlParser parser = new CCJSqlParser(new StringReader(line));
					
					try{
						if((statement = parser.Statement()) != null){
							if(statement instanceof Select){
								SelectBody select = ((Select) statement).getSelectBody();
								ExpressionTree e = new ExpressionTree();
								if (select instanceof PlainSelect){
									Operator op = e.generateTree(select);
									while (op.peekNextOp() != null){
										op = op.peekNextOp();
										System.out.println(op);
									}
									System.out.println(op.readOneTuple());
								}
							}
							else if(statement instanceof CreateTable){
								CreateTable createTableObj = (CreateTable) statement;								
								prepareTableSchema(createTableObj);
							}
						}
					}
					catch (Exception e){
						System.out.println(e);
					}
				}
			}
			catch (IOException ex){
				System.out.println("There was an IO error"+ ex.getMessage());
			}
		}
	}
/**
 * (non javaDocs)
 * prepares table schema information and saves it in a static hashmap 
 * @param createTableObj createTableObject from jsql parser
 * @author Shiva
 */
		private static void prepareTableSchema(CreateTable createTableObj){		
			@SuppressWarnings("unchecked")
			List<ColumnDefinition> cds = (List<ColumnDefinition>) createTableObj.getColumnDefinitions();
			
			//creates a map colIndex of {colName : [colData Type, Index]} and adds it to final map tableMappings being {tableName : {colName : [colData Type, Index]} }									
	//		HashMap<String, ArrayList<String>> colIndexDataType = new HashMap<String, ArrayList<String>>(); 
	//		int colCount = 0;
	//		for(ColumnDefinition colDef : cds){
	//			ArrayList<String> strArr = new ArrayList<String>();
	//			strArr.add(colDef.getColDataType().toString());
	//			strArr.add(Integer.toString(colCount));
	//			
	//			colIndexDataType.put(colDef.getColumnName(),strArr );
	//			
	//			colCount++;
	//		}
	//		tableMappings.put(createTableObj.getTable().getWholeTableName(), colIndexDataType);
			
			HashMap<Integer, String> colIndexDataType_Map = new HashMap<Integer, String>(); 
			HashMap<String, Integer> colNameIndex_Map = new HashMap<String, Integer>(); 
			
			int colCount = 0;
			for(ColumnDefinition colDef : cds){
				ArrayList<String> strArr = new ArrayList<String>();
				strArr.add(colDef.getColDataType().toString());
				strArr.add(Integer.toString(colCount));
				
				colIndexDataType_Map.put(colCount, colDef.getColDataType().toString());
				colNameIndex_Map.put(colDef.getColumnName(), colCount);
				
				colCount++;
			}
			
			ArrayList<HashMap<?,?>> ColIndexNameMap_list = new ArrayList<HashMap<?,?>>();
			ColIndexNameMap_list.add(colIndexDataType_Map); 
			ColIndexNameMap_list.add(colNameIndex_Map);
			
			tableMappings.put(createTableObj.getTable().getWholeTableName(),ColIndexNameMap_list);		
		}
		
		/**	 
		 * test code print
		 */
		static void println(String string) {
			// TODO Auto-generated method stub
			System.out.println(string);
		}
		
		static void printTuple(Datum[] singleTuple) {
			for(int i=0; i < singleTuple.length;i++){
				System.out.print(singleTuple[i].value.toString());
				if(i != singleTuple.length - 1) System.out.print("|");
			}
			System.out.println();
		}	
	}