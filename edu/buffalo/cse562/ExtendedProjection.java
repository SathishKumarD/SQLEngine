/**
 * 
 */
package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * @author Sathish
 *
 */
public class ExtendedProjection implements Operator {

	Operator input;
	List<SelectItem> SelectItem_List;
	private  HashMap<String, ColumnDetail> inputSchema = null;
	
	private  HashMap<String, ColumnDetail> outputSchema = null; // this will be given to SubQueries!	
	//private List<String> outputColumnExpressions_List = new ArrayList<String>();
	ArrayList<Tuple> inputTuples = null;
	private ArrayList<Tuple> outputTuples = new ArrayList<Tuple> ();
	
	public ExtendedProjection(Operator input, List<SelectItem> SelectItem_List) {
		this.input = input;
		this.SelectItem_List = SelectItem_List;		
		this .inputSchema = input.getOutputTupleSchema();
		
		//reset();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
 	public ArrayList<Tuple> readOneTuple() {
		do{
			inputTuples = input.readOneTuple(); 
			outputTuples.removeAll(outputTuples);
			
			for(SelectItem selectItem : SelectItem_List)	
			{
				if(selectItem instanceof AllColumns)
				{
					outputTuples.addAll(inputTuples);
				}
				else if(selectItem instanceof AllTableColumns)
				{
					Set<Integer> tableColumnIndex = new HashSet();
					
					String tableName = ((AllTableColumns) selectItem).getTable().getName();
					for(Entry<String, ColumnDetail> es : inputSchema.entrySet()){
						if(es.getKey().contains(tableName))
						{
							tableColumnIndex.add(es.getValue().getIndex());
						}
						
						for(Iterator<Integer> itr = tableColumnIndex.iterator(); itr.hasNext(); ){
							int index = itr.next();
							outputTuples.add(inputTuples.get(index));
						}
					}				
				}
				
				else if(selectItem instanceof SelectExpressionItem)
				{
					Expression expr = ((SelectExpressionItem) selectItem).getExpression();
					
					if(expr instanceof Function)
					{				
						String key = expr.toString();
						if(inputSchema.containsKey(key))
						{
						   int index = inputSchema.get(expr.toString()).getIndex(); //.get(column.getWholeColumnName()).getIndex();
						   outputTuples.add(inputTuples.get(index));
						}					   							 
					}
					else 
					{
						Evaluator evaluator = new Evaluator(inputTuples, inputSchema);					
						try {
							outputTuples.add(new Tuple(evaluator.eval(expr)));
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}			
	
			}		
			return outputTuples;
		} while(inputTuples != null);
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	//sets the outputTupleSchema
	@Override
	public void reset() {
		int index = 0;
		for(SelectItem selectItem : SelectItem_List)		
		{
			if(selectItem instanceof AllColumns){
				// *
				for(Entry<String, ColumnDetail> es : inputSchema.entrySet()){
					String key = es.getKey();
					if(!outputSchema.containsKey(key)){
						ColumnDetail colDetail = es.getValue().clone();
						colDetail.setIndex(index);
						outputSchema.put(key, colDetail);	
						
						index++;
					}
				}
			}			
			else if (selectItem instanceof AllTableColumns) {
				//<tableName>.*
				String tableName = ((AllTableColumns) selectItem).getTable().getName();
				for(java.util.Map.Entry<String, ColumnDetail> es : inputSchema.entrySet()){
					String keyStr = es.getKey();
					if(keyStr.contains(tableName.concat("."))) {
						if(!outputSchema.containsKey(keyStr)){
							ColumnDetail colDetail = inputSchema.get(keyStr).clone();
							colDetail.setIndex(index);
							outputSchema.put(keyStr, es.getValue());
							
							index++;
						}
					}
				}				
			}			
			else if(selectItem instanceof SelectExpressionItem){				
				//<columnName> or <columnName> AS <columnAlias> or <columnA + columnB> [AS <columnAlias>]'expression
				String aliasName = ((SelectExpressionItem) selectItem).getAlias();
				
				//alias name is present!
				if(aliasName != null  &&  !aliasName.isEmpty()){
					Expression exp = ((SelectExpressionItem) selectItem).getExpression();
					String colName = exp.toString();

					if(!outputSchema.containsKey(colName)){										
						if(inputSchema.containsKey(colName)){
						
						ColumnDetail colDetail = inputSchema.get(colName).clone();
						colDetail.setIndex(index);						
						
						outputSchema.put(colName, colDetail);						
						//add additional schema for alias names as well
						outputSchema.put(aliasName, colDetail);
						}												
						else
						{ //if its a expression... ex: A+B , C*D 
							// a new column not found in previous schema basically an expression							
							ColumnDetail colDetail = new ColumnDetail();
							colDetail.setIndex(index);														
							
							outputSchema.put(colName, colDetail);	
							outputSchema.put(aliasName, colDetail);
						}
						
						index++;
					}
				}
				else{//alias name is not present
					Expression exp = ((SelectExpressionItem) selectItem).getExpression();
					String colName = exp.toString();
					
					if(!outputSchema.containsKey(colName)){																
						
						if(inputSchema.containsKey(colName)){
							ColumnDetail colDetail = inputSchema.get(colName).clone();
							colDetail.setIndex(index);						
							outputSchema.put(colName, colDetail);
						}									
						else 
						{ // a new column not found in previous schema basically an expression
							// //if its a expression... A+B , C*D 
							ColumnDetail colDetail = new ColumnDetail();
							colDetail.setIndex(index);														
							
							outputSchema.put(colName, colDetail);							
						}						
						
						index++;
					}					
				}
				
			}					
		}		
	}
	
	public String toString(){
		return "SELECT " + this.SelectItem_List.toString();
	}
	
	public Operator peekNextOp(){
		return this.input;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Here input schema gets changed. columns will be removed.
		return null;
	}
}
