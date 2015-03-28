package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import net.sf.jsqlparser.statement.select.OrderByElement;

public class ExternalSortOperator implements Operator {
	Operator child;
	File swapDir;
	LinkedHashMap<Integer, Boolean> sortFields;
	Comparator<ArrayList<Tuple>> comp;
	int bufferLength;
	HashMap<String, ColumnDetail> outputSchema;
	private static final int BUFFER_SIZE = 10000;
	TreeMap<Integer, String> typeMap;
	List<ArrayList<Tuple>> workingSet;
	boolean sorted = false;
	MiniScan outputStream;
	boolean ascending;
	ArrayList<Tuple> lastFlushed;
	List<OrderByElement> orderByElements;
	Operator parentOperator = null;
	public ExternalSortOperator(Operator child, List<OrderByElement> orderByElements) {
		// TODO Auto-generated constructor stub
		swapDir = new File(ConfigManager.getSwapDir(), UUID.randomUUID().toString());
		if (!swapDir.exists()){
			swapDir.mkdir();
		}
		this.outputSchema = child.getOutputTupleSchema();
		this.sortFields = new LinkedHashMap<Integer, Boolean>(orderByElements.size());

		for (OrderByElement ob : orderByElements){
//			System.out.println(ob);
			//int index = this.outputSchema.get(ob.getExpression().toString()).getIndex();
			int index = 0;
			try
			{
			 index = Evaluator.getColumnDetail(child.getOutputTupleSchema(),ob.getExpression().toString().toLowerCase()).getIndex();
			}
			catch(Exception ex)
			{
				System.err.println("Error in getting index for column:  " + ob.getExpression().toString().toLowerCase());
				System.err.println("parent: " + this.getParent());
				System.err.println("current: " + this);
				System.err.println("child: " + child);
				throw ex;
				
			}
			sortFields.put(index, ob.isAsc());
		}

		this.orderByElements = orderByElements;
		this.child = child;

		this.comp = new TupleComparator(sortFields);

		//Number of string objects, not number of tuples
		this.bufferLength = Math.floorDiv(BUFFER_SIZE, this.getOutputTupleSchema().size());
		this.typeMap = new TreeMap<Integer, String>();		
		for (ColumnDetail c : outputSchema.values()){
			typeMap.put(c.getIndex(), c.getColumnDefinition().getColDataType().toString().toLowerCase());
		}
	}

	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		ArrayList<Tuple> currentTuple;


		if (!sorted){
			long start = new Date().getTime();
			twoWaySort();
			sorted = true;
			// System.out.println("==== Sorted in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
		}


		//##########	
		//Finally, return tuples from sorted file
		currentTuple = outputStream.readTuple();	
		if (currentTuple != null){
			return currentTuple;
		}

		return null;
	}

	private void twoWaySort(){
		ArrayList<Tuple> currentTuple;
		this.workingSet = new ArrayList<ArrayList<Tuple>>(this.bufferLength);
		int index = 0;
		int nPass = 0;
		File currentFileHandler = getFileHandle(index, nPass);
		
		// First run; sorts input tuples in batches, and writes to separate files on disk
		while((currentTuple = child.readOneTuple())!= null){
			if (addToSet(currentTuple, true, currentFileHandler)){
				index = index + 1;
				currentFileHandler = getFileHandle(index, nPass);
			}
		}
		index = index + 1;
		currentFileHandler = getFileHandle(index, nPass);
		flushWorkingSet(currentFileHandler, true);
		// System.out.println("Working set now " +workingSet.size());
		mergeFull(currentFileHandler, index, nPass);
	}

	private void mergeFull(File currentFileHandler, int size, int nPass){		
		//This can be changed to a different base for N-way sort; Merge method also has to be changed
		size = (int) Math.pow(2, Math.ceil(Math.log(size)/Math.log(2)));
		// Merging
		File fName1;
		File fName2;
		for (int s = size; s > 0; s /= 2){
			int lcIndex = 0;
			for (int i = 0; i < s; i += 2){
				fName1 = getFileHandle(i, nPass);
				fName2 = getFileHandle(i+1, nPass);
				currentFileHandler = getFileHandle(lcIndex++, nPass + 1) ;
				mergeOnce(fName1, fName2, currentFileHandler);
			}
			nPass = nPass + 1;
		}
		try {
			outputStream = new MiniScan(currentFileHandler, typeMap);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void mergeOnce(File ifName1, File ifName2, File ofName){
		try {
			MiniScan left = new MiniScan(ifName1, typeMap);
			MiniScan right = new MiniScan(ifName2, typeMap);
			ArrayList<Tuple> leftTup = left.readTuple();
			ArrayList<Tuple> rightTup = right.readTuple();

			//Merge procedure
			while (!(leftTup == null) && !(rightTup == null)){
				if (this.comp.compare(leftTup, rightTup) > 0){
					addToSet(leftTup, false, ofName);
					leftTup = left.readTuple(); 
				}
				else{
					addToSet(rightTup, false, ofName);
					rightTup = right.readTuple();
				}
			}

			//flush what's left to disk
			while (leftTup != null){
				addToSet(leftTup, false, ofName);
				leftTup = left.readTuple();
			}

			while (rightTup != null){
				addToSet(rightTup, false, ofName);
				rightTup = right.readTuple();
			}
			
			//cleanup
			flushWorkingSet(ofName, false);			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	private boolean addToSet(ArrayList<Tuple> toAdd, boolean sort, File currentFileHandle){
		/* adds a tuple to the working set and also returns a flag indicating
		 * whether or not the working set was flushed (this is particularly useful for updating
		 * file indexes)
		 */
		if (workingSet.size() < this.bufferLength){
			workingSet.add(toAdd);
			return false;
		}
		else{
		//	System.err.println(" Collecting garbage with " 
		//			+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1000000 +"MB currently used");
			flushWorkingSet(currentFileHandle, sort);
			workingSet.add(toAdd);	
			return true;
		}
	}

	private boolean flushWorkingSet(File currFileHandle, boolean sorted){
		if (sorted){
			Collections.sort(workingSet, this.comp);
		}
		writeToDisk(workingSet, currFileHandle);
		System.gc();
		// System.out.println("Memory used: " + 
		// 		((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1000000) + "MB");
		workingSet = new ArrayList<ArrayList<Tuple>>(this.bufferLength);
		return true;
	}

	private boolean writeToDisk(List<ArrayList<Tuple>> out, File writeDir){
		PrintWriter pw;	

		try {			
			//append to file; useful for merging, and ensures that there is never a fileNotFound exception
			pw = new PrintWriter(new FileWriter(writeDir, true));
			for(ArrayList<Tuple> t : out){
				Util.printToStream(t, pw);
			}
			pw.close();
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	//TODO make the buffered writer a singleton object
	private boolean writeOneToDisk(ArrayList<Tuple> out, File writeDir){
		PrintWriter pw;	

		try {			
			//append to file; useful for merging, and ensures that there is never a fileNotFound exception
			pw = new PrintWriter(new BufferedWriter(new FileWriter(writeDir, true)));
			Util.printToStream(out, pw);
			pw.close();
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private File getFileHandle(int index, int nPass){
		String fname = nPass+"-"+index;
		File writeDir = new File(this.swapDir, fname);

		if (!writeDir.exists()){
			try {
				writeDir.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return writeDir;
	}

	private void replacementSort(){
		ArrayList<Tuple> currentTuple = child.readOneTuple();
		int nRun = 1;
		this.workingSet = new LinkedList<ArrayList<Tuple>>();
		int flushed = 0;		
		lastFlushed = currentTuple;

		//for subsequent runs
		while (currentTuple != null){
			if (this.comp.compare(currentTuple, lastFlushed) >= 0){
				System.out.println("Inserting one!");
				writeOneToDisk(currentTuple, getFileHandle(0, nRun));
				lastFlushed = currentTuple;
			}

			else{
				workingSet.add(currentTuple);
			}

			if (workingSet.size() - 1 > this.bufferLength){
				nRun = nRun + 1;
				Collections.sort(workingSet, this.comp);			
				System.out.println("Flushing working set " +workingSet.size());
				flushed = appendToOutput(workingSet, nRun);
				System.out.println("Flushed " +flushed+ " tuples");
			}
			currentTuple = child.readOneTuple();
		}

		mergeFull(getFileHandle(0, nRun), nRun, 0);

	}

	private int appendToOutput(List<ArrayList<Tuple>> workingSet, int nRun){
		int nFlushed = 0;
		for (int i = 0; i < workingSet.size(); i++){
			ArrayList<Tuple> tup = workingSet.get(i);
			//			System.out.println("Comparing " + lastFlushed + " and " +tup);
			if (lastFlushed == null){
				lastFlushed = tup;
			}
			if (this.comp.compare(lastFlushed, tup) <= 0){
				//				System.out.println("YES!!");
				lastFlushed = tup;
				writeOneToDisk(tup, getFileHandle(0, nRun));
				workingSet.remove(i);
				nFlushed = nFlushed + 1;
			}
		}
		return nFlushed;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return this.outputSchema;
	}	


	public Comparator<ArrayList<Tuple>> getComp() {
		return comp;
	}

	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return child;
	}

	@Override
	public void setChildOp(Operator child) {
		// TODO Auto-generated method stub
		// System.out.println("changing child of external sort");
		this.child = child;
		child.setParent(this);
		this.outputSchema = child.getOutputTupleSchema();
	}

	@Override
	public Operator getParent() {
		return this.parentOperator;
	}

	@Override
	public void setParent(Operator parent) {
		this.parentOperator = parent;		
	}

	public String toString(){
		return "External Sort on  " + orderByElements ;
	}

}
