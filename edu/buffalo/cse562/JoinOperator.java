/**
 * 
 */
package edu.buffalo.cse562;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
	private Expression expr = null;
	private ArrayList<Tuple> leftTuple;
	private ArrayList<Tuple> rightTuple;

	public JoinOperator(Operator left, Operator right, Expression expr){
		this.left = left;
		this.right = right;
		this.expr = expr;
		String str = null;
		str.toCharArray();
		this.reset();
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		ArrayList<Tuple> outputTuple = new ArrayList<Tuple>();
		rightTuple = right.readOneTuple();		

		if (rightTuple == null){
			right.reset();
			rightTuple = right.readOneTuple();
			this.reset();
		}
		
		if (leftTuple == null){
			return null;
		}

		int posCount = outputTuple.size();
		boolean returnThis = true;

		for (Map.Entry<String, ColumnDetail> mp : left.getOutputTupleSchema().entrySet()){
			int index = mp.getValue().getIndex();
			Tuple value = leftTuple.get(index);
			outputTuple.add(value);
		    outputSchema.get(mp.getKey()).setIndex(posCount);
		    posCount += 1;
		}

		for (Map.Entry<String, ColumnDetail> mp : right.getOutputTupleSchema().entrySet()){
			int index = mp.getValue().getIndex();
			Tuple value = rightTuple.get(index);
			outputTuple.add(value);
		    outputSchema.get(mp.getKey()).setIndex(posCount);
		    posCount += 1;
		}
			
		if (this.expr != null){
			Evaluator evaluator = new Evaluator(outputTuple, outputSchema);
			try {
				returnThis = ((BooleanValue) evaluator.eval(this.expr)).getValue();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(returnThis){
			return outputTuple;
		}
		
		else{
			return readOneTuple();
		}
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		outputSchema = new HashMap<String, ColumnDetail>();
		leftSchema = new HashMap<String, ColumnDetail>(left.getOutputTupleSchema());
		rightSchema = new HashMap<String, ColumnDetail>(right.getOutputTupleSchema());
		for (Entry<String, ColumnDetail> en : rightSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue().clone();
			outputSchema.put(key, value);
		}
		for (Entry<String, ColumnDetail> en : leftSchema.entrySet()){
			String key = en.getKey();
			ColumnDetail value = en.getValue().clone();
			outputSchema.put(key, value);
		}
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
	
}
