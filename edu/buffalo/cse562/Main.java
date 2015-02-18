package edu.buffalo.cse562;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
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


public class Main {
	
	// static HashMap<String, ArrayList<HashMap<?,?>>> tableMappings = new HashMap<String, ArrayList<HashMap<?,?>>>();
	   static HashMap<String, HashMap<String, ColumnDetail>> tableMapping = new HashMap<String, HashMap<String, ColumnDetail>>();
	 
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
									/*while (op.peekNextOp() != null){
										op = op.peekNextOp();
										System.out.println(op);
									}
									System.out.println(op.readOneTuple());*/
									ExecuteQuery(op);
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
			String tableName = createTableObj.getTable().getWholeTableName();
			List<ColumnDefinition> cds = (List<ColumnDefinition>) createTableObj.getColumnDefinitions();
			HashMap<String, ColumnDetail> tableSchema = new HashMap<String, ColumnDetail>();
			int colCount = 0;
			for(ColumnDefinition colDef : cds){
				ColumnDetail columnDetail = new ColumnDetail();
				columnDetail.setTableName(tableName);
				columnDetail.setColumnDefinition(colDef);
				columnDetail.setIndex(colCount);
				String columnFullName = tableName + "."+ colDef.getColumnName();
				columnDetail.setIndex(colCount);
				tableSchema.put(columnFullName, columnDetail);
				colCount++;
			}
			tableMapping.put(tableName,tableSchema);		
		}
		
		/**	 
		 * test code print
		 */
		static void println(String string) {
			// TODO Auto-generated method stub
			System.out.println(string);
		}
		
		static void printTuple(ArrayList<Tuple> singleTuple) {
			for(int i=0; i < singleTuple.size();i++){
				System.out.print(singleTuple.get(i).toString());
				if(i != singleTuple.size() - 1) System.out.print("|");
			}
			System.out.println();
		}	
		
		static void ExecuteQuery(Operator op)
		{
			ArrayList<Tuple> dt=null;
			do
			{
				dt = op.readOneTuple();
				if(dt !=null) printTuple(dt);
				
			}while(dt!=null);
			
		}	
		
	}