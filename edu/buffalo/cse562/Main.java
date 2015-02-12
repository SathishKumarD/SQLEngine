package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class Main {
	public static void main(String[] args) {
//		System.out.println("We, the members of our team, agree that we will not submit any code that we have not written ourselves, share our code with anyone outside of our group, or use code that we have not written ourselves as a reference.");
		//test
		Path sqlFile = FileSystems.getDefault().getPath(args[0]);
		Charset charset = Charset.forName("US-ASCII");
		Statement statement;
		HashMap<String, Integer> columnDefs = new HashMap<String, Integer>();
		
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
								System.out.println(op);
								while (op.peekNextOp() != null){
									op = op.peekNextOp();
									System.out.println(op);
								}
							}
						}
						else if(statement instanceof CreateTable){
							CreateTable createStatement = (CreateTable) statement;
							@SuppressWarnings("unchecked")
							List<ColumnDefinition> cds = (List<ColumnDefinition>) createStatement.getColumnDefinitions();
						}
					}
				}
				catch (Exception e){
					System.out.println(e);
				}

			}
		}
		catch (IOException ex){
			System.out.println("There was an IO error");
		}
	}
}