/**
 * 
 */
package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import edu.buffalo.cse562.Eval;

/**
 * @author Sathish
 *
 */
public class Evaluator extends Eval {
	
	ArrayList<Tuple> tuple;
	
	public Evaluator(ArrayList<Tuple> tuple)
	{
		this.tuple = tuple;
		
	}

	@Override
	public LeafValue eval(Column column) throws SQLException {
		return null;
		// TODO Auto-generated method stub
		
		
		
	}

}
