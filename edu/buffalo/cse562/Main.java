package edu.buffalo.cse562;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import java.util.HashMap;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

import java.util.ArrayList;


public class Main {

	static HashMap<String, HashMap<String, ColumnDetail>> tableMapping = new HashMap<String, HashMap<String, ColumnDetail>>();
	static HashMap<String,HashMap<Integer, String>> indexTypeMaps = new HashMap<String, HashMap<Integer, String>>();

	static int queryCount = 0;
	public static void main(String[] args) {		
		//the sql file starts from 3rd argument
		if(args.length < 3){
			System.out.println("Incomplete arguments");
			return;
		}

		//TODO shiva testing
		if (args[0].equals("--data")){
			ConfigManager.setDataDir(args[1]);
		}
		int sqlIndex = 2;

		if (args[2].equals("--swap")){
			ConfigManager.setSwapDir(args[3]);
			sqlIndex = 4;
		}

		ArrayList<File> queryFiles = new ArrayList<File>();

		for(int i=sqlIndex; i < args.length; i++){	
			queryFiles.add(new File(args[i]));
		}
		Statement statement =null;						

		for (File f : queryFiles){		
			try{
				CCJSqlParser parser = new CCJSqlParser(new FileReader(f));
				ExpressionTree e = new ExpressionTree();
				while ((statement = parser.Statement()) != null){
					//					System.out.println(statement);
					if(statement instanceof Select){
						SelectBody select = ((Select) statement).getSelectBody();

						if (select instanceof PlainSelect){
							// 	System.err.println(select);
							Operator op = e.generateTree(select);
							try
							{
							System.out.println("______________________________________");
							System.out.println("	Old Execution Plan");
							System.out.println("______________________________________");
							printPlan(op);
							System.out.println("______________________________________");
							System.out.println("	Old Execution Plan's Result");
							System.out.println("______________________________________");
//							ExecuteQuery(op);							
							System.out.println("______________________________________");
								
							System.out.println("	Optimized Execution Plan");
							System.out.println("______________________________________");
							new QueryOptimizer(op);	
							printPlan(op);
							System.out.println("______________________________________");
							System.out.println("	Optimized Execution Plan's Result");
							System.out.println("______________________________________");
							
							ExecuteQuery(op);													
							}
							catch(Exception ex)
							{
								System.err.println("ERROR MSG");
								System.err.println(select);
								System.out.println(ex.getMessage());
								ex.printStackTrace();
							}
						}
						else if (select instanceof Union){
							Union un = (Union) select;
							Operator op;
							UnionOperator uop = new UnionOperator();
							List<PlainSelect> pselects = (List<PlainSelect>) un.getPlainSelects();
							for (PlainSelect s : pselects){
								uop.addOperator(e.generateTree(s));
							}
							ExecuteQuery(uop);
						}

					}
					else if(statement instanceof CreateTable){
						CreateTable createTableObj = (CreateTable) statement;								
						prepareTableSchema(createTableObj);
					}
				}
			}
			catch(Exception e){
				System.err.println(statement);
				e.printStackTrace();
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
		String[] tableNames = new String[1];
		String tableName = createTableObj.getTable().getWholeTableName().toLowerCase();

		List<ColumnDefinition> cds = (List<ColumnDefinition>) createTableObj.getColumnDefinitions();
		HashMap<String, ColumnDetail> tableSchema = new HashMap<String, ColumnDetail>();
		HashMap<Integer, String> typeInfo = new HashMap<Integer, String>();
		int colCount = 0;
		for(ColumnDefinition colDef : cds){
			ColumnDetail columnDetail = new ColumnDetail();
			columnDetail.setTableName(tableName);
			columnDetail.setColumnDefinition(colDef);
			columnDetail.setIndex(colCount);

			String columnFullName = tableName + "."+ colDef.getColumnName();

			typeInfo.put(colCount, colDef.getColDataType().getDataType()); //indexMaps : {tableName:{columnIndex:columnType}}

			tableSchema.put(columnFullName, columnDetail);
			colCount++;
		}
		tableMapping.put(tableName,tableSchema);
		indexTypeMaps.put(tableName,typeInfo);
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

			try
			{
				String str = (singleTuple.get(i)==null)?"":singleTuple.get(i).toString();
				System.out.print(str);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.out.println(singleTuple.get(i));
			}

			if(i != singleTuple.size() - 1) System.out.print("|");
		}
		System.out.println();
	}	

	static String getTupleAsString(ArrayList<Tuple> singleTuple) {

		StringBuilder sb = new  StringBuilder();
		for(int i=0; i < singleTuple.size();i++){

			try
			{
				String tupleStr = (singleTuple.get(i)==null)?"":singleTuple.get(i).toString();
				sb.append(tupleStr);
				//System.out.print(str);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.out.println(singleTuple.get(i));
			}

			if(i != singleTuple.size() - 1) sb.append("|");
		}
		sb.append("\n");
		return sb.toString();
		// System.out.println();
	}	

	static void ExecuteQuery(Operator op)
	{
		ArrayList<Tuple> dt=null;
		StringBuilder sb = new StringBuilder();
		do
		{
			dt = op.readOneTuple();
			if(dt !=null)
			{
				//printTuple(dt);
				sb.append(getTupleAsString(dt)); 
			}

		}while(dt!=null);

		System.out.print(sb.toString());			
	}	

	static void printPlan(Operator op)
	{
		while(op!=null)
		{
			System.out.println(op.toString());
			op = op.getChildOp();
		}
	}	

}
