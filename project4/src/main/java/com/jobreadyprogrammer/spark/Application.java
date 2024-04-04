package com.jobreadyprogrammer.spark;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class Application {

	public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

//		ArrayToDataset app = new ArrayToDataset();
//		app.start();

//		CsvToDatasetHouseToDataframe app = new CsvToDatasetHouseToDataframe();
//		app.start();

		WordCount wc = new WordCount();
		wc.start();

	}


}
