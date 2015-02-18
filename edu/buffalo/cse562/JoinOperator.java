/**
 * 
 */
package edu.buffalo.cse562;
import edu.buffalo.cse562.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
	private Expression expr = null;
	private ArrayList<Tuple> leftTuple;
	private ArrayList<Tuple> rightTuple;

	public JoinOperator(Operator left, Operator right, Expression expr){
		outputSchema = new HashMap<String, ColumnDetail>(left.getOutputTupleSchema());
		HashMap<String, ColumnDetail> rightSchema = right.getOutputTupleSchema();
		for (Entry<String, ColumnDetail> en : rightSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue();
			outputSchema.put(key, value);
		}
		this.left = left;
		this.right = right;
		this.expr = expr;
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		
		ArrayList<Tuple> outputTuple = new ArrayList<Tuple>();
		
		if (rightTuple == null){
			leftTuple = left.readOneTuple();
			right.reset();
		}
		
		rightTuple = right.readOneTuple();

		if (leftTuple == null){
			return null;
		}
		
		// Cross product expression
		if (this.expr == null){
//			System.out.println(this.outputSchema);
			ArrayList<Tuple> rightTuple = right.readOneTuple();
			
			for (Map.Entry<String, ColumnDetail> mp : left.getOutputTupleSchema().entrySet()){
				int index = mp.getValue().getIndex();
				Tuple value = leftTuple.get(index);
				int outputIndex = outputSchema.get(mp.getKey()).getIndex();
				outputTuple.add(outputIndex, value);
			}
			
			for (Map.Entry<String, ColumnDetail> mp : right.getOutputTupleSchema().entrySet()){
				int index = mp.getValue().getIndex();
				Tuple value = rightTuple.get(index);
				int outputIndex = outputSchema.get(mp.getKey()).getIndex();
				outputTuple.add(outputIndex, value);
			}
		}
		return outputTuple;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
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
	
}
