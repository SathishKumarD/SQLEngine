package edu.buffalo.cse562;

public class ConfigManager {
	private static String DATA_DIR;
	
	public static void setDataDir(String dir){
		DATA_DIR = dir;
	}
	
	public static String getDataDir(){
		return DATA_DIR;
	}
}
