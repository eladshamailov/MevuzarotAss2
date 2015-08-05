import java.io.IOException;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.cloudfront.model.InvalidArgumentException;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class ExtractCollocations {

	public static void main(String[] args) throws IOException, InterruptedException {
		double minPmi, relMinPmi;
		boolean removeStopWords;

		//validate input arguments
		if (args == null || args.length != 4 )
			throw new InvalidArgumentException("Invalid number of arguments");
		
		try
		{
			minPmi = Double.parseDouble(args[0]);
			relMinPmi = Double.parseDouble(args[1]);
			removeStopWords = Boolean.parseBoolean(args[3]);
		}
		catch(NumberFormatException e)
		{
			InvalidArgumentException toThrow= new InvalidArgumentException("The argument "+ args[0] +" isn't an integer");
			toThrow.initCause(e);
			throw toThrow;
		}
		
		// get AWS credentials and initiate the ElasticMapReduce instance and s3 instance
		AWSCredentials credentials = new PropertiesCredentials(ExtractCollocations.class.getResourceAsStream("AwsCredentials.properties"));
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);
		AmazonS3 s3 = new AmazonS3Client(credentials);
		String bucketName = "ass2-yoav-eliran";
		
		// change connection timeout to infinity to 
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		clientConfiguration.setConnectionTimeout(0);
		clientConfiguration.setSocketTimeout(0);

		String corpus = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"; //Hebrew corpus

		//*********************************Job1 configuration***********************************************
		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
		    .withJar("s3n://" + bucketName + "/Job1.jar")
		    .withMainClass("Job1.Job1Main")
		    .withArgs(corpus, "s3n://" + bucketName + "/outputJob1/", Boolean.toString(removeStopWords));
		 
		StepConfig stepConfig1 = new StepConfig()
		    .withName("Job1")
		    .withHadoopJarStep(hadoopJarStep1)
			.withActionOnFailure("TERMINATE_JOB_FLOW");

		//*********************************Job2 configuration***********************************************
		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
				.withJar("s3n://" + bucketName + "/Job2.jar")
				.withMainClass("Job2.Job2Main")
				.withArgs("s3n://" + bucketName + "/outputJob1", "s3n://" + bucketName + "/outputJob2");

		StepConfig stepConfig2 = new StepConfig()
				.withName("Job2")
				.withHadoopJarStep(hadoopJarStep2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		//*********************************Job3 configuration***********************************************
		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
				.withJar("s3n://" + bucketName + "/Job3.jar")
				.withMainClass("Job3.Job3Main")
				.withArgs("s3n://" + bucketName + "/outputJob2/" ,"s3n://" + bucketName + "/outputJob3/");

		StepConfig stepConfig3 = new StepConfig()
				.withName("Job3")
				.withHadoopJarStep(hadoopJarStep3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
				.withJar("s3n://" + bucketName + "/Job4.jar")
				.withMainClass("Job4.Job4Main")
				.withArgs("s3n://" + bucketName + "/outputJob3/", "s3n://" + bucketName + "/outputJob4/", Double.toString(minPmi), Double.toString(relMinPmi));

		StepConfig stepConfig4 = new StepConfig()
				.withName("Job4")
				.withHadoopJarStep(hadoopJarStep4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		//*********************************JobFlow configuration***********************************************
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(10)
				.withMasterInstanceType(InstanceType.M1Large.toString())
				.withSlaveInstanceType(InstanceType.M1Large.toString())
				.withHadoopVersion("2.2.0").withEc2KeyName("yoaveliran")
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
		    .withName("ExtractCollocations")
		    .withInstances(instances)
			.withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4)
			.withLogUri("s3n://" + bucketName + "/logs/");

		runFlowRequest.setServiceRole("EMR_DefaultRole");
		runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");

		ClusterSummary theCluster = null;

		ListClustersRequest clustRequest = new ListClustersRequest().withClusterStates(ClusterState.WAITING);

		ListClustersResult clusterList = mapReduce.listClusters(clustRequest);
		for (ClusterSummary cluster : clusterList.getClusters()) {
			if (cluster != null)
				theCluster = cluster;
		}

		if (theCluster != null) {
			AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
					.withJobFlowId(theCluster.getId())
					.withSteps(stepConfig1, stepConfig2, stepConfig3, stepConfig4);
			AddJobFlowStepsResult runJobFlowResult = mapReduce.addJobFlowSteps(request);
			String jobFlowId = theCluster.getId();
			System.out.println("Ran job flow with id: " + jobFlowId);
		} else {
			RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
			String jobFlowId = runJobFlowResult.getJobFlowId();
			System.out.println("Ran job flow with id: " + jobFlowId);
		}
	}

}
