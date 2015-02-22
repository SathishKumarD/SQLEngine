/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Sathish
 *
 */
public class GroupByOutput {
	private int count;
	private HashMap<String,ArrayList<Tuple>> outputData;
	
	public GroupByOutput()
	{
		this.setCount(0);
		this.setOutputData(new HashMap<String, ArrayList<Tuple>>());
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public HashMap<String,ArrayList<Tuple>> getOutputData() {
		return outputData;
	}

	public void setOutputData(HashMap<String,ArrayList<Tuple>> outputData) {
		this.outputData = outputData;
	}

}
