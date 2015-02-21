/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

/**
 * @author Sathish
 *
 */
public class GroupByOperator implements Operator {

	private  HashMap<String, ColumnDetail> inputSchema = null;
	private Operator input;
	private List<Column> groupByColumns;
	private List<Function> aggregateunctions;
	
	
	public GroupByOperator(Operator input, List<Column> groupByColumns,
			List<Function> aggregateunctions) {
		this.input = input;
		this.inputSchema = input.getOutputTupleSchema();
		this.groupByColumns = groupByColumns;
		this.aggregateunctions = aggregateunctions;
		
		// TODO Auto-generated constructor stub
	}

	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public Operator peekNextOp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return  this.inputSchema;
	}

}
