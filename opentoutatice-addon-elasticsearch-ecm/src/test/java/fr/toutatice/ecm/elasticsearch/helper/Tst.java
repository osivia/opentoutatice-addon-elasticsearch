package fr.toutatice.ecm.elasticsearch.helper;

import org.opentoutatice.elasticsearch.core.reindexing.docs.ReIndexingRunnerStepStateStatus;

public class Tst {

	public static void main(String[] args) {
		
		ReIndexingRunnerStepStateStatus statusA = null;
		ReIndexingRunnerStepStateStatus statusB = null;
		
		System.out.println(statusA == statusB);
		System.out.println(ReIndexingRunnerStepStateStatus.inError.equals(statusB));

	}

}
