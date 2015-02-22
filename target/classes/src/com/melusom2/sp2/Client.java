//***********************************************
// Michael Meluso
// CSC 470 - CLoud Computing
// Project 2: AWS S3 Client
// 
// Client class
// Runs sample code to test functionality
// of AWS Java API calls to S3
//**********************************************

package src.com.melusom2.sp2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.StorageClass;

public class Client {

    /**
     * Pauses the program; waits for user input to continue
     */
    private static void pause() {
        System.out.println("Press Any Key To Continue...");
          new java.util.Scanner(System.in).nextLine();
    }

    /**
     * Overloaded pause method that supplies custom message
     */
    private static void pause(String message) {
        System.out.println(message);
          new java.util.Scanner(System.in).nextLine();
    }

    /**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to Amazon S3
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createSampleFile() throws IOException {
        File file = File.createTempFile("aws-java-sdk-", ".txt");
        file.deleteOnExit();

        Writer writer = new OutputStreamWriter(new FileOutputStream(file));
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.write("01234567890112345678901234\n");
        writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
        writer.write("01234567890112345678901234\n");
        writer.write("abcdefghijklmnopqrstuvwxyz\n");
        writer.close();

        return file;
    }

    /**
     * Displays the contents of the specified input stream as text.
     *
     * @param input
     *            The input stream to display as text.
     *
     * @throws IOException
     */
    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

	public static void run() throws IOException {
		/*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            // Change the config file to another file
            credentials = new ProfilesConfigFile("profiles.txt").getCredentials("default");
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location, and is in valid format.",
                    e);
        }

        AmazonS3 s3 = new AmazonS3Client(credentials);
        String oldBucketName = "tcnj-csc470-melusom2";
        String newBucketName = "tcnj-csc470-mikes-new-bucket";
        String mikeBucket = "michaelmeluso.com";
        String key = "MyNewObject";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");

        try {
            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
            System.out.println("Uploading a new object to previously existing S3 bucket \"" + oldBucketName + "\"...\n");
            s3.putObject(new PutObjectRequest(oldBucketName, key, createSampleFile()));
            System.out.println("Object \"" + key + "\" added to bucket " + oldBucketName + "!\n");
            pause();
            /*
             * Download an object - When you download an object, you get all of
             * the object's metadata and a stream from which to read the contents.
             * It's important to read the contents of the stream as quickly as
             * possibly since the data is streamed directly from Amazon S3 and your
             * network connection will remain open until you read all the data or
             * close the input stream.
             *
             * GetObjectRequest also supports several other options, including
             * conditional downloading of objects based on modification times,
             * ETags, and selectively downloading a range of an object.
             */
            System.out.println("Fetching the newly added object...\n");
            S3Object object = s3.getObject(new GetObjectRequest(oldBucketName, key));
            System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent());
            System.out.println("Object get successful!\n");
            pause();
            /*
             * Delete an object - Unless versioning has been turned on for your bucket,
             * there is no way to undelete an object, so use caution when deleting objects.
             */
            System.out.println("Deleting that object now...\n");
            s3.deleteObject(oldBucketName, key);
            System.out.println("Object \"" + key + "\" deleted from " + oldBucketName + "!\n");
            pause();
            /*
             * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
             * so once a bucket name has been taken by any user, you can't create
             * another bucket with that same name.
             *
             * You can optionally specify a location for your bucket if you want to
             * keep your data closer to your applications or users.
             */
            System.out.println("Creating bucket \"" + newBucketName + "\"...\n");
            s3.createBucket(newBucketName);
            System.out.println("Bucket " + newBucketName + " created!\n");
            pause();
            /*
             * Test that the current user can access an Amazon S3 Bucket.
             * If successful, returns the AccessControlList for the bucket.
             * Otherwise, returns an error. 
             *
             * This bucket the user has access to.
             */
            System.out.println("Performing HEAD call on a bucket user has access to...\n");
            AccessControlList acl = s3.getBucketAcl(oldBucketName);
            System.out.println("Bucket " + oldBucketName + " returned:\n");
            System.out.println("Bucket owner: " + acl.getOwner() + "\n");
            System.out.println("Bucket grants: " + acl.getGrants() + "\n");
            pause();
            /*
             * Test that the current user can access an Amazon S3 Bucket.
             * If successful, returns the AccessControlList for the bucket.
             * Otherwise, returns an error. 
             *
             * This bucket the user does NOT have access to.
             */
            System.out.println("Performing HEAD call on a bucket user doesn't have access to...\n");
            try {
                AccessControlList acl2 = s3.getBucketAcl(mikeBucket);  
            } catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException as expected, because the user does not "
                        + "have access to the bucket \"" + mikeBucket + "\".");
                System.out.println("Error Message: " + ace.getMessage() + "\n"); 
            }
            pause();
            /*
             * Put an object into a bucket that will expire 5 days in the future.
             * Also sets storage class to Reduced Redundancy.
             */
            Transition transToArchive = new Transition()
                .withDays(4)
                .withStorageClass(StorageClass.Glacier);

            BucketLifecycleConfiguration.Rule ruleArchiveAndExpire = new BucketLifecycleConfiguration.Rule()
                .withId("Archive and delete rule")
                .withPrefix("projectdocs/")
                .withTransition(transToArchive)
                .withExpirationInDays(5)
                .withStatus(BucketLifecycleConfiguration.ENABLED.toString());

            List<BucketLifecycleConfiguration.Rule> rules = new ArrayList<BucketLifecycleConfiguration.Rule>();
            rules.add(ruleArchiveAndExpire);

            BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration()
                .withRules(rules);

            // Save configuration.
            s3.setBucketLifecycleConfiguration(newBucketName, configuration);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setExpirationTimeRuleId("Archive and delete rule");
            System.out.println("Adding a new object \"" + key + "\" to bucket \"" + newBucketName + "\"...\n");
            System.out.println("Object is set to expire in five (5) days.");
            String newKey = "projectdocs/" + key;
            PutObjectRequest request = new PutObjectRequest(newBucketName, newKey, createSampleFile())
                .withStorageClass(StorageClass.ReducedRedundancy)
                .withMetadata(meta);
            s3.putObject(request);
            System.out.println("Object \"" + newKey + "\" added to bucket " + newBucketName + " with set expiration and storage class!\n");
            pause();
            /*
             * List all buckets
             */
            System.out.println("Listing all of your buckets...\n");
            List<Bucket> bList = s3.listBuckets();
            for(Iterator<Bucket> iter = bList.iterator(); iter.hasNext();) {
                Bucket item = iter.next();
                System.out.println(item.toString() + "\n");
            }
            System.out.println("Finished listing your buckets!\n");
            pause();
            /*
             * Copy object from one bucket to another bucket
             */
            System.out.println("Copying object \"" + newKey + "\" from bucket \"" + newBucketName + "\" to bucket \"" + oldBucketName + "\"...\n");
            s3.copyObject(newBucketName, newKey, oldBucketName, key);
            System.out.println("\"" + newKey + "\" from \"" + newBucketName + "\" successfully copied to \"" + key + "\" in \"" + oldBucketName + "\"!\n");
            pause();
            /*
             * Gets HEAD (metadata) of an object
             */
            System.out.println("Executing HEAD request on the newly copied object...\n");
            ObjectMetadata objectHead = s3.getObjectMetadata(new GetObjectMetadataRequest(oldBucketName, key));
            System.out.println("Content-Type: "  + objectHead.getContentType());
            System.out.println("Version: "  + objectHead.getVersionId());
            System.out.println("Expiration rule: "  + objectHead.getExpirationTimeRuleId() + "\n");
            System.out.println("Object HEAD successful!\n");
            pause();
            /*
             * Put versioning on a bucket
             */
            System.out.println("Enabling versioning on bucket \"" + oldBucketName + "\"...\n");
            BucketVersioningConfiguration buc = new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED);
            SetBucketVersioningConfigurationRequest verQuest = new SetBucketVersioningConfigurationRequest(oldBucketName, buc);
            s3.setBucketVersioningConfiguration(verQuest);
            System.out.println("Versioning enabled on bucket \"" + oldBucketName + "\"!\n");
            /*
             * Put several versions of a file into a bucket
             */
            System.out.println("Putting three additional versions of \"" + key + "\" into \"" + oldBucketName + "\"...\n");
            PutObjectResult v1 = s3.putObject(new PutObjectRequest(oldBucketName, key, createSampleFile()));
            pause("Version 1 added. Giving S3 a brief rest. Press any key to continue...");
            PutObjectResult v2 =s3.putObject(new PutObjectRequest(oldBucketName, key, createSampleFile()));
            pause("Version 2 added. Giving S3 a brief rest. Press any key to continue...");
            PutObjectResult v3 =s3.putObject(new PutObjectRequest(oldBucketName, key, createSampleFile()));
            pause("Version 3 added. Giving S3 a brief rest. Press any key to continue..."); 
            /*
             * Delete a specific version of an object
             */
            System.out.println("Deleting version 2 from before of \"" + key + "\" from \"" + oldBucketName + "\"...\n");
            s3.deleteVersion(new DeleteVersionRequest(oldBucketName, key, v2.getVersionId()));
            System.out.println("\"" + key + "\" with version ID of " + v2.getVersionId() + " successfully deleted from \"" + oldBucketName + "\"!\n");
            /*
             * Delete all files created during this program
             */
            System.out.println("Cleaning up...\n");
            s3.deleteObject(oldBucketName, key);
            s3.deleteObject(newBucketName, newKey);
            /*
             * Delete a bucket - A bucket must be completely empty before it can be
             * deleted, so remember to delete any objects from your buckets before
             * you try to delete them.
             */
            System.out.println("Deleting bucket \"" + newBucketName + "\"...\n");
            s3.deleteBucket(newBucketName);
            System.out.println("Bucket " + newBucketName + " deleted!\n");
            pause();

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

        System.out.println("===========================================");
        System.out.println("Program terminating");
        System.out.println("===========================================\n");
	}
}
