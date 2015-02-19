/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.sun.corba.se.impl.encoding.OSFCodeSetRegistry.Entry;

import net.sf.jsqlparser.expression.Expression;
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
	private ArrayList<Tuple> outputTuples = null;
	private  HashMap<String, ColumnDetail> outputSchema = null;
	
	public ExtendedProjection(Operator input, List<SelectItem> SelectItem_List) {
		this.input = input;
		this.SelectItem_List = SelectItem_List;		
		this .inputSchema = input.getOutputTupleSchema();
		reset();
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
 	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		
		return input.readOneTuple();
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
				for(java.util.Map.Entry<String, ColumnDetail> es : inputSchema.entrySet()){
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
				
				if(aliasName != null  &&  !aliasName.isEmpty()){
					Expression exp = ((SelectExpressionItem) selectItem).getExpression();
					String colName = exp.toString();
					 //TODO remove @shiva
					if(!outputSchema.containsKey(colName){
						ColumnDetail colDetail = inputSchema.get(colName).clone();
						colDetail.setIndex(index);						
						outputSchema.put(colName, colDetail);
						
						index++;
					}
				}
				else{
					
				}
				
				//((SelectExpressionItem) selectItem).getAlias()
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
