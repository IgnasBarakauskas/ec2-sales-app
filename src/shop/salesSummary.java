package shop;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class salesSummary {
	public static void main(String[] args) {
			Region region = Region.US_EAST_1;
			String queueURL = "https://sqs.us-east-1.amazonaws.com/057004900367/salesAppQueue";
			S3Client s3 = S3Client.builder().region(region).build();
			SqsClient sqsClient = SqsClient.builder().region(region).build();
			while (true) {
			Message message = ReceiveMessages(queueURL, sqsClient);
			if(message !=null) {
			System.out.println(message);
			String messageString = message.body();
			String[] data = messageString.split(";");
			String bucketName = data[0].toString();
			String fileName = data[1].toString();
			System.out.println("Message information: Bucket name: " + bucketName + "\nFile name: " + fileName);
			try {
				DownloadFile(bucketName, fileName, s3);
			} catch (IOException e) {
				e.printStackTrace();
			}
			calculate(s3, bucketName, fileName);
			deleteMessage(queueURL, message, sqsClient);
			deleteFile(fileName);
			}
		}
	}

//	Calculates final value
	public static void calculate(S3Client s3, String bucketName, String fileName) {
		BufferedReader reader;
		double totalProfit = 0.0;
		int count = 0;
		ArrayList<Product> products = new ArrayList<Product>();
		try {
			reader = new BufferedReader(new FileReader(fileName));
			String line = reader.readLine();
			while (line != null) {
				if (count > 0) {
					String[] cols = line.split(";");
					String nameToMatch = cols[2];
					totalProfit += Double.parseDouble(cols[6]);

					try {
						int index = IntStream.range(0, products.size())
								.filter(i -> nameToMatch.equals(products.get(i).getName())).findFirst().getAsInt();

						// update existing
						Product existingProduct = products.get(index);

						Product newProduct = new Product(cols[2], Double.parseDouble(cols[3]),
								Double.parseDouble(cols[4]), Double.parseDouble(cols[6]));

						existingProduct.incrementAll(newProduct);
						products.set(index, existingProduct);

					} catch (NoSuchElementException e) {
						// create new product
						Product newProduct = new Product(cols[2], Double.parseDouble(cols[3]),
								Double.parseDouble(cols[4]), Double.parseDouble(cols[6]));
						products.add(newProduct);

					}
				}
				count++;
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		writeToCSV(totalProfit, products, "summary-" + fileName);
		uploadBucket(s3, bucketName, "summary-" + fileName);
		DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(fileName)
				.build();

		s3.deleteObject(deleteObjectRequest);

	}
// Upload summary file to bucket
	public static void uploadBucket(S3Client s3, String bucketName, String fileName) {
		PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(fileName).build();
		s3.putObject(request, RequestBody.fromFile(new File(fileName)));
		System.out.println("FileAdded");
		deleteFile(fileName);
	}
// Creates CSV file locally
	public static boolean writeToCSV(double totalProfit, ArrayList<Product> products, String fileName) {

		FileWriter csvWriter;
		try {
			csvWriter = new FileWriter(fileName);

			csvWriter.append("Total Profit");
			csvWriter.append(";");
			csvWriter.append(Double.toString(totalProfit));
			csvWriter.append("\n");
			csvWriter.append("\n");

			csvWriter.append("Product");
			csvWriter.append(";");
			csvWriter.append("TotalQuantity");
			csvWriter.append(";");
			csvWriter.append("TotalPrice");
			csvWriter.append(";");
			csvWriter.append("TotalProfit");
			csvWriter.append("\n");

			for (Product product : products) {
				csvWriter.append(product.getName());
				csvWriter.append(";");
				csvWriter.append(Double.toString(product.getQuantity()));
				csvWriter.append(";");
				csvWriter.append(Double.toString(product.getPrice()));
				csvWriter.append(";");
				csvWriter.append(Double.toString(product.getProfit()));
				csvWriter.append("\n");
			}

			csvWriter.flush();
			csvWriter.close();

			System.out.println("Writing done for summary file:" + fileName);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
//deletes file from local storage
	public static void deleteFile(String fileName) {
		File f = new File(fileName);
		f.delete();
		System.out.println("File delete locally");
	}
// receives messages
	public static Message ReceiveMessages(String queueUrl, SqsClient sqsClient) {

		ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueUrl)
				.maxNumberOfMessages(5).build();
		List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
		if (messages.size() == 0) {
			return null;
		}
		return (messages.get(0));
	}

	// Check if bucket exist
	private static boolean BucketExist(S3Client s3, String bucketName) {
		ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
		ListBucketsResponse listBucketResponse = s3.listBuckets(listBucketsRequest);
		return listBucketResponse.buckets().stream().anyMatch(x -> x.name().equals(bucketName));
	}

//	Download file for future work
	public static void DownloadFile(String bucketName, String fileName, S3Client s3) throws FileNotFoundException {
		if (BucketExist(s3, bucketName)) {
			GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(fileName).build();

			ResponseInputStream<GetObjectResponse> response = s3.getObject(getObjectRequest);
			BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(fileName));
			byte[] buffer = new byte[4096];
			int bytesRead = -1;
			try {
				while ((bytesRead = response.read(buffer)) != -1) {
					outputStream.write(buffer, 0, bytesRead);
				}
				response.close();
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("File downloaded");
		}
	}

	// Deletes messages from SQS
	public static void deleteMessage(String queueUrl, Message message, SqsClient sqsClient) {
		DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueUrl)
				.receiptHandle(message.receiptHandle()).build();
		sqsClient.deleteMessage(deleteMessageRequest);
		System.out.println("Message was deleted");
	}
}
