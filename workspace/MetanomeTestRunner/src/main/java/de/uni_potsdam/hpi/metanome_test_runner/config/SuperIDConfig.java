package de.uni_potsdam.hpi.metanome_test_runner.config;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class SuperIDConfig {

	public enum Dataset {
		PLANETS, SYMBOLS, SCIENCE, SATELLITES, GAME, ASTRONOMICAL, ABALONE, ADULT, BALANCE, BREAST, BRIDGES, CHESS, ECHODIAGRAM, FLIGHT, HEPATITIS, HORSE, IRIS, LETTER, NURSERY, PETS, NCVOTER_1K, NCVOTER_1K_OWN, UNIPROD_1K
	}
	
	public List<Dataset> datasets = new LinkedList<Dataset>();
	public List<InputFileConfig> inputFileConfigs = new LinkedList<InputFileConfig>();
	
	public String measurementsFolderPath = "io" + File.separator + "measurements" + File.separator;
	
	public String statisticsFileName = "statistics.txt";
	public String resultFileName = "results.txt";
	
	public boolean writeResults = true;
	
	public static SuperIDConfig create(String[] args) {
		return new SuperIDConfig();
	}
	
	public SuperIDConfig() {
		this(SuperIDConfig.Dataset.PLANETS, SuperIDConfig.Dataset.SATELLITES); //NCVOTER_1K
	}

	public SuperIDConfig(SuperIDConfig.Dataset... datasets) {
		for(SuperIDConfig.Dataset dataset : datasets)
		{
			this.addDataset(dataset);
		}	
	}

	@Override
	public String toString() {
		
		String datasets = "";
		for(InputFileConfig inputFile : this.inputFileConfigs)
		{
			datasets += inputFile.datasetName + inputFile.fileEnding + "\r\n\t";
		}
		
		return "Config:\r\n\t" +
			"algorithm: " + "SuperID" + "\r\n\t" +
			"datasets: " + "\r\n\t" + datasets;
	}

	private void addDataset(SuperIDConfig.Dataset dataset) {
		InputFileConfig inputFileConfig = new InputFileConfig();
		this.datasets.add(dataset);
		inputFileConfigs.add(inputFileConfig);
		switch (dataset) {
			case PLANETS:
				inputFileConfig.datasetName = "WDC_planets";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
				break;
			case SYMBOLS:
				inputFileConfig.datasetName = "WDC_symbols";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
				break;
			case SCIENCE:
				inputFileConfig.datasetName = "WDC_science";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
				break;
			case SATELLITES:
				inputFileConfig.datasetName = "WDC_satellites";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
				break;
			case GAME:
				inputFileConfig.datasetName = "WDC_game";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
				break;
			case ASTRONOMICAL:
				inputFileConfig.datasetName = "WDC_astronomical";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
				break;
			case ABALONE:
				inputFileConfig.datasetName = "abalone";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case ADULT:
				inputFileConfig.datasetName= "adult";
				inputFileConfig.separator = ';';
				inputFileConfig.hasHeader = false;
				break;
			case BALANCE:
				inputFileConfig.datasetName = "balance-scale";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case BREAST:
				inputFileConfig.datasetName = "breast-cancer-wisconsin";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case BRIDGES:
				inputFileConfig.datasetName = "bridges";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case CHESS:
				inputFileConfig.datasetName = "chess";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case ECHODIAGRAM:
				inputFileConfig.datasetName = "echocardiogram";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case FLIGHT:
				inputFileConfig.datasetName = "flight_1k";
				inputFileConfig.separator = ';';
				inputFileConfig.hasHeader = true;
				break;
			case HEPATITIS:
				inputFileConfig.datasetName = "hepatitis";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case HORSE:
				inputFileConfig.datasetName = "horse";
				inputFileConfig.separator = ';';
				inputFileConfig.hasHeader = false;
				break;
			case IRIS:
				inputFileConfig.datasetName = "iris";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case LETTER:
				inputFileConfig.datasetName = "letter";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case NURSERY:
				inputFileConfig.datasetName = "nursery";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = false;
				break;
			case PETS:
				inputFileConfig.datasetName = "pets";
				inputFileConfig.separator = ';';
				inputFileConfig.hasHeader = true;
				break;
			case NCVOTER_1K:
				inputFileConfig.datasetName = "ncvoter_1001r_19c";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
			case NCVOTER_1K_OWN:
				inputFileConfig.datasetName = "ncvoter-1k";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
				inputFileConfig.quotechar = '"';
				break;
			case UNIPROD_1K:
				inputFileConfig.datasetName = "uniprot_1001r_223c";
				inputFileConfig.separator = ',';
				inputFileConfig.hasHeader = true;
				break;
		}
	}
}
