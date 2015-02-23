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
	private Expression expr = null;
	private ArrayList<Tuple> leftTuple;
	private ArrayList<Tuple> rightTuple;

	public JoinOperator(Operator left, Operator right, Expression expr){
		this.left = left;
		this.right = right;
		this.expr = expr;
		this.reset();
	}
	
	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		TreeMap<Integer, Tuple> outputMap = new TreeMap<Integer, Tuple>();
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

		boolean returnThis = true;
		outputMap = populateTuple(outputMap, left.getOutputTupleSchema(), leftTuple);
		outputMap = populateTuple(outputMap, right.getOutputTupleSchema(), rightTuple);
		
		outputTuple = treeMapToList(outputMap);
			
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
	
	private ArrayList<Tuple> treeMapToList(TreeMap<Integer, Tuple> in){
		ArrayList<Tuple> res = new ArrayList<Tuple>();
		for (Tuple t : in.values()){
			res.add(t);
		}
		return res;
	}
	
	private TreeMap<Integer, Tuple> populateTuple(TreeMap<Integer, Tuple> current, 
			HashMap<String, ColumnDetail> schema, ArrayList<Tuple> thisTuple){
		for (Map.Entry<String, ColumnDetail> mp : schema.entrySet()){
			int index = mp.getValue().getIndex();
			Tuple value = thisTuple.get(index);
			int outdex = outputSchema.get(mp.getKey()).getIndex();
			current.put(outdex, value);
		}
		return current;
	}
	
}
