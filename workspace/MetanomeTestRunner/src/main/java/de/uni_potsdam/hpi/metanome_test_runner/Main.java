package de.uni_potsdam.hpi.metanome_test_runner;

import de.uni_potsdam.hpi.metanome_test_runner.config.Config;
import de.uni_potsdam.hpi.metanome_test_runner.config.SuperIDConfig;
import de.uni_potsdam.hpi.metanome_test_runner.mocks.MetanomeMock;

public class Main {

	public static void main(String[] args) {
		SuperIDConfig conf = SuperIDConfig.create(args);
		MetanomeMock.execute(conf);
	}

}
