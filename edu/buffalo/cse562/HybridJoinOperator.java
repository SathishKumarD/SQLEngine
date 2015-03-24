package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;

public abstract class HybridJoinOperator<T> implements Operator{
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
	private Expression expr;
	private int leftIndex;
	private int rightIndex;
	private HashMap<String, ArrayList<Tuple>> joinHash;
	boolean hashed = false;

	public HybridJoinOperator(Operator left, Operator right, String columnLeft, String columnRight){
		/* Since only equijoins are supported, this operator only requires
		 * the operators to be joined and the corresponding columns for each 
		 * operator
		 */
		this.left = left;
		this.right = right;	
		this.expr = expr;
		joinHash = new HashMap<String, ArrayList<Tuple>>();		
		//TODO: get the objects of the expression and find the corresponding indexes in each tuple
		leftIndex = left.getOutputTupleSchema().get(columnLeft).getIndex();
		rightIndex = right.getOutputTupleSchema().get(columnRight).getIndex();
		
		generateOutputSchema();
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
//		leftTuple = left.readOneTuple();
		
		if (!hashed){
			rightTuple = right.readOneTuple();		
	
			while(rightTuple != null){
				joinHash.put(rightTuple.get(rightIndex).toString(), rightTuple);
				rightTuple = right.readOneTuple();
			}
			hashed = true;
		}
		
		leftTuple = left.readOneTuple();
		if (leftTuple != null){
			ArrayList<Tuple> hashedRight = joinHash.get(leftTuple.get(leftIndex));
			if (hashedRight != null){
				leftTuple.addAll(hashedRight);
				return leftTuple;
			}
			else{
				return readOneTuple();
			}
		}
		return null;
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
