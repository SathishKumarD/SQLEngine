package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
	private static final int BUFFER_SIZE = 1000;
	TreeMap<Integer, String> typeMap;
	List<ArrayList<Tuple>> workingSet;
	boolean sorted = false;
	MiniScan outputStream;
	boolean ascending;
	List<OrderByElement> orderByElements;
	
	public ExternalSortOperator(Operator child, List<OrderByElement> orderByElements) {
		// TODO Auto-generated constructor stub
		swapDir = new File(ConfigManager.getSwapDir(), UUID.randomUUID().toString());
		if (!swapDir.exists()){
			swapDir.mkdir();
		}
		this.outputSchema = child.getOutputTupleSchema();
		this.sortFields = new LinkedHashMap<Integer, Boolean>(orderByElements.size());
		
		for (OrderByElement ob : orderByElements){
//			String fullFieldName = getFullField(ob.getExpression().toString());
			System.out.println(ob);
			//System.out.println(this.outputSchema);
			int index = this.outputSchema.get(ob.getExpression().toString()).getIndex();
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
		
		// First run; sorts input tuples in batches, and writes to separate files on disk
		
		if (!sorted){
			long start = new Date().getTime();
			twoWaySort();
			sorted = true;
			System.out.println("==== Sorted in " + ((new Date().getTime() - start)/ 1000) + "s");
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
		while((currentTuple = child.readOneTuple())!= null){
			if (addToSet(currentTuple, true, currentFileHandler)){
				index = index + 1;
				currentFileHandler = getFileHandle(index, nPass);
			}
		}
		index = index + 1;
		currentFileHandler = getFileHandle(index, nPass);
		flushWorkingSet(currentFileHandler, true);
		System.out.println("Working set now " +workingSet.size());
		
		//This can be changed to a different base for N-way sort; Merge method also has to be changed
		int size = (int) Math.pow(2, Math.ceil(Math.log(index)/Math.log(2)));
		
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
//			System.err.println(" Collecting garbage with " 
//					+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1000000 +"MB currently used");
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
		System.out.println("Memory used: " + 
					((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1000000) + "MB");
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
		int nRun = 100;
		this.workingSet = new LinkedList<ArrayList<Tuple>>();
		
		writeOneToDisk(currentTuple, getFileHandle(nRun, nRun));
		ArrayList<Tuple> lastFlushed = currentTuple;

		//for subsequent runs
		while (currentTuple != null){
			workingSet.add(currentTuple);
			Collections.sort(workingSet, this.comp);			
			//May not be necessary, as both reference the same object
			lastFlushed = appendToOutput(workingSet, lastFlushed, nRun);
			
			if (workingSet.size() - 1 > this.bufferLength){
//				nRun = nRun + 1;
				lastFlushed = appendToOutput(workingSet, workingSet.get(0), nRun);
			}
			currentTuple = child.readOneTuple();
		}
	}
	
	private ArrayList<Tuple> appendToOutput(List<ArrayList<Tuple>> workingSet, ArrayList<Tuple> lastFlushed, int nRun){
		int nFlushed = 0;
		for (int i = 0; i < workingSet.size(); i++){
			ArrayList<Tuple> tup = workingSet.get(i);
			if (this.comp.compare(lastFlushed, tup) >= 0){
				lastFlushed = tup;
				writeOneToDisk(tup, getFileHandle(nRun, nRun));
				workingSet.remove(i);
				nFlushed = nFlushed + 1;
			}
		}
		return lastFlushed;
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return child.getOutputTupleSchema();
	}
	
	private class TupleComparator implements Comparator<ArrayList<Tuple>>{
		LinkedHashMap<Integer, Boolean> sortFields;
		public TupleComparator(LinkedHashMap<Integer, Boolean> sortFields){
			this.sortFields = sortFields;
		}

		@Override
		public int compare(ArrayList<Tuple> o1,
				ArrayList<Tuple> o2) {
			// TODO Auto-generated method stub
			int diff = 0;
			for (Map.Entry<Integer, Boolean> mp : sortFields.entrySet()){
				if (mp.getValue()){
					diff = o1.get(mp.getKey()).compareTo(o2.get(mp.getKey()));
				}
				else {
					diff = o2.get(mp.getKey()).compareTo(o1.get(mp.getKey()));
				}
				
				if (diff != 0){
					return diff;
				}
			}
			return diff;
		}
	}
	
	
	private class MiniScan{
		BufferedReader br;
		String line;
		TreeMap<Integer, String> typeMap;

		public MiniScan(File filename, TreeMap<Integer, String> typeMap) throws IOException{
			Charset charset = Charset.forName("US-ASCII");
			this.br = Files.newBufferedReader(filename.toPath(), charset);
			this.typeMap = typeMap;
		}
		
		private ArrayList<Tuple> parseLine(String raw){
			String col[] = line.split("\\|");	
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			for(Map.Entry<Integer, String> entry : typeMap.entrySet()) {
				tuples.add(new Tuple(entry.getValue(), col[entry.getKey()]));	
			}
			return tuples;
		}
		
		public ArrayList<Tuple> readTuple(){
			try {
				if ((line = br.readLine())!= null){
					return parseLine(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				this.br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;			
		}
	}

	private class ReturnObject{
		int flushed;
		ArrayList<Tuple> lastFlushed;
		
		public ReturnObject(int flushed, ArrayList<Tuple> lastFlushed) {
			// TODO Auto-generated constructor stub
			this.flushed = flushed;
			this.lastFlushed = lastFlushed;
		}
	}
	
	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return child;
	}

	@Override
	public void setChildOp(Operator child) {
		// TODO Auto-generated method stub
		this.child = child;
	}

	@Override
	public Operator getParent() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setParent(Operator parent) {
		// TODO Auto-generated method stub
		
	}
	
	public String toString(){
		return "External Sort on  " + orderByElements ;
	}

}
