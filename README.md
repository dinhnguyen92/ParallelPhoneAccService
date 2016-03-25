# ParallelPhoneAccService
Simple program to retrieve phone accounts from AppSheet database and display 5 accounts with the youngest age and valid US phone number

**Project requirements**

Make a C# program that outputs the 5 youngest users with valid US telephone numbers sorted by name

The program must employ multi-threading/parallelism to reduce run time

The program must employ LINQ to handle lists


**AppSheet Web Service**

The web service for accessing the phone account database is provided by AppSheet.
Service Endpoint: https://appsheettest1.azurewebsites.net/sample/

Web Service Methods:

1. list: 
This method will return an array of up to 10 user IDs.  
If there are more than 10 results the response will also contain a token that can be used to retrieve the next set of results.  
This optional token can be passed as a query string parameter
Eg:  https://appsheettest1.azurewebsites.net/sample/list or https://appsheettest1.azurewebsites.net/sample/list?token=b32b3

2. detail/{user id}:
This method will returns the full details for a given user
Eg:  https://appsheettest1.azurewebsites.net/sample/detail/21


**Project's main classes**

1. Program.cs: the program class containing the Main method
2. PhoneAcc.cs: class for representing phone account objects retrieved from database using the detail/{user id} method
3. IDList.cs: class for representing the list of account IDs retrieved from database using the list method


**Algorithm Overview**

Before implementing multi-threading, we must first identify which parts of the problem are parallelizable.
The given problem can be roughly divided into 3 sub-problems: 

1. Retrieving all the account IDs from the web service
2. Retrievinng all the accounts using the obtained IDs from the web service
3. Finding 5 accounts with smallest age from among the retrieved accounts

In the first sub-problem, the IDs of the accounts must first be retrieved before the accounts can be retrieved using the IDs.
The IDs can only be retrieved in batches of at most 10. At each time, only 1 batch can be downloaded since only one token can be obtained at any one time.
As such, retrieving the IDs from the web service is sequential task.

On the other hand, in the second sub-problem once a batch of IDs has been received, the corresponding accounts can be downloaded concurrently.
Hence this sub-problem can be easily parallelized.

For the third sub-problem, suppose we keep a list of accounts that are potentially the final accounts to be selected. 
Assuming that the account with the oldest age in this list is known, then the third sub-problem simply requires us to compare 
this account with any new account retrieved. If the new account has a younger age, then we swap the 2 accounts.
Multiple threads can carry out this task in parallel independently, making the third sub-problem parallelizable.
However, the assumption that the account with oldest age is known presents a tricky situation:
each time the list of potential results changes, the list must be scanned or sorted in order to find the new account with oldest age.
This operation cannot be parallelized, which reduces the degree of parallelism of the third sub-problem.

Having identified the parallel and sequential parts of the problem, we now have 2 approaches to solving the problem:

1. Sequentially download and store all of the account IDs before retrieving and processing the accounts in parallel
2. Download the account IDs in a separate thread. 
At the same time, retrieve and process the accounts in parallel on the fly using already downloaded IDs.
Once an ID or an account has been processed, it is discarded.

Clearly, the 2nd approach is the better approach, as it stores fewer IDs and performs more tasks in parallel.
However, this approach is still seriously limited by the senquential downloading of IDs: 
If the rate at which IDs are downloaded is slower than the rate at which accounts are retrieved and processed,
then there will be idle threads waiting for IDs to work with, and the program is only as fast as the speed of downloading IDs.

This means that the program can slow down considerably if the database is large, or during periods of heavy traffic, 
which slows down the program's download rate. 

Also, even though the second approach uses less memory than the first approach, it still uses more memory than a single-threaded solution.
At anytime, a single-threaded solution only needs to store a temporary copy of an account. 
In constrast, since each thread in a multi-threaded solution has its own copy of an account, more memory is needed.

Finally, it should be noted that the third sub-problem described earlier is not perfectly parallel. 
Each time a thread attempts to write to or modify the list of potential results, it must use mutex/lock to prevent race condition.
Since other threads are prevented from accessing the list, this operation is essentially sequential. 
If this sequential operation occurs fairly often (for example, the accounts retrieved by the threads might be in generally decreasing 
order of age as a result of some unknown ordering in the database), the program can be almost as slow as a single-threaded solution.


**Parallelism Implementation**

A separate thread is spawn to retrieve the account IDs in the background.

While the IDs are being retrieved, a Parallel.foreach loop is used to process the retrieved ID in each ID list. 
Using Parallel.foreach, each ID is assigned to one thread, which uses the ID to retrieve the account in parallel. 

In this first version of ParallelPhoneAccService, once the account has been retrieved, the thread will determine whether to 
add the account to the list of results. If the account is added, the thread will sort the list using LINQ so that the account
with the oldest age can be identified.
The sorting of the list increases the waiting time by other threads and potentially slows down the program.


**Effect of Parallelism**

The multi-threaded solution generally runs faster than the single-threaded solution.

The mutli-threaded solution takes about 1.3 - 1.5 sec to process the same data set as compared to the 2 - 2.3 sec required by the single-threaded solution.





