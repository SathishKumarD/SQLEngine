/**
 * 
 */
package edu.buffalo.cse562;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;

/**
 * @author Sathish
 *
 */
public class JoinOperator implements Operator {

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	
	//TODO: Create setters and getters
	private Operator left;
	private Operator right;
	private HashMap<String, ColumnDetail> outputSchema = null;
	private HashMap<String, ColumnDetail> leftSchema;
	private HashMap<String, ColumnDetail> rightSchema;
	private ArrayList<Tuple> leftTuple;
	private ArrayList<Tuple> rightTuple;

	public JoinOperator(Operator left, Operator right, Expression expr){
		this.left = left;
		this.right = right;		
		generateOutputSchema();
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
//		leftTuple = left.readOneTuple();
		rightTuple = right.readOneTuple();		

		if (rightTuple == null){
			right.reset();
			rightTuple = right.readOneTuple();
			this.reset();
		}
		
		if (leftTuple == null){
			return null;
		}

		leftTuple.addAll(rightTuple);
		return leftTuple;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.leftTuple = left.readOneTuple();
	}
	
	public String toString(){
		StringBuilder b = new StringBuilder("JOIN WITH \n");
		b.append('\t' +this.right.toString() + '\n');
		return b.toString();
	}
	
	public Operator peekNextOp(){
		return this.left;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return this.outputSchema;
	}

	private void generateOutputSchema(){
		outputSchema = new HashMap<String, ColumnDetail>();
		leftSchema = new HashMap<String, ColumnDetail>(left.getOutputTupleSchema());
		rightSchema = new HashMap<String, ColumnDetail>(right.getOutputTupleSchema());
		int offset = 0;
		for (Entry<String, ColumnDetail> en : rightSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue().clone();
			int index = value.getIndex();
			if (index > offset){
				offset = index;
			}
			outputSchema.put(key, value);
		}
		for (Entry<String, ColumnDetail> en : leftSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue().clone();
			int index = value.getIndex();
			value.setIndex(index + offset + 1);
			outputSchema.put(key, value);
		}
	}
	
}
