using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop.WebHDFS.Adapters;
using Microsoft.Hadoop.WebHDFS;
using Microsoft.Hadoop.Hive;


namespace WebHDFS
{
    class Program
    {
        static void Main(string[] args)
        {
            //need to perform foll substitution to match ur env
            //replace {Azurestorageaccount} with root name of ur strage accnt
            var asvstorageaccountname = "shavas300";
            var asvkey = "W00+SmVFMQffNUikJpfAa8WOGdhka27fP42dnRG2FhuCxH4Mk7TaN2qGknqMwNjCXvbKCMwHeLJZWByhWiO4sQ==";
            var clustername = "shavas100";
            var hadoopusername = "admin";
            var hadoopuserpassword = "Ekdantay12!@";
            //            var clusteruri = "https://" + clustername + ".azurehdinsight.net"; //build cluster uri with full address

            var studentid = "03";
            var asvstudentdirectory = "/" + studentid + "_webhdfs/crimeresult";
            var hivetablename = studentid + "_crimeinfo_net";
            var hivetablepathincontainer = "/" + studentid + "_crimeinfo";
            var crimedatafile = "/CityCrimeResultData.csv";
            var localfile = "c:/data" + crimedatafile;

            // blobl storage account detail

            var asvaccount = asvstorageaccountname + ".blob.core.windows.net";
            var asvcontainer = "working";
            var clusteruri = "https://" + clustername + ".azurehdinsight.net";

            // setup azure storage for the program
            var storageadapter = new BlobStorageAdapter(asvaccount, asvkey, asvcontainer, true);
            var HDFSClient = new WebHDFSClient(hadoopusername, storageadapter);
            Console.WriteLine("Creating directory: " + asvstudentdirectory + " in account: " + asvstorageaccountname + "container: " + asvcontainer );
            
//create directory and wait for the task to complete
            HDFSClient.CreateDirectory(asvstudentdirectory).Wait();
            Console.WriteLine("Copying");
//The below command will copy the files from local directory to azure storage
            HDFSClient.CreateFile(localfile, asvstudentdirectory + crimedatafile).Wait();

            //Create Hive Connection
            var hiveconnection = new MyHiveDatabase(new System.Uri(clusteruri), hadoopusername, hadoopuserpassword, asvaccount, asvkey);
            Console.WriteLine("creating hive table  " + hivetablename);

            // Create table on storage
            string command = "CREATE TABLE " + hivetablename;
            command +=
            @"(	state string,
	city string,
	population string,
	total_criminal_activities string,
	crime_percentage string
   ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ";
            command += "LOCATION 'wasb://" + asvcontainer + "@" + asvaccount + hivetablepathincontainer + "';";

            Console.WriteLine(command);
            hiveconnection.ExecuteHiveQuery(command).Wait();
            Console.WriteLine(" TABLE CREATED SUCCESSFULLY.... ");
            Console.WriteLine("LOADING DATA FROM ....." + crimedatafile + " into the '" + hivetablename + "' table in hive ...");

            //LOAD DATA FROM FILE TO TABLE
            command = "LOAD data inpath 'wasb://";
            command += asvcontainer + "@" + asvaccount + asvstudentdirectory + crimedatafile + "' OVERWRITE INTO TABLE " + hivetablename + ";";
            Console.WriteLine(command);
            hiveconnection.ExecuteHiveQuery(command).Wait();
            Console.WriteLine("Performing Hive Query .... ");
            //EXECUTE QUERY TO GET DATA FROM CREATED TABLE
            command = "SELECT * FROM " + hivetablename + ";";
            Console.WriteLine(command);
            var result = hiveconnection.ExecuteQuery(command);

            //show the output
            Console.WriteLine("THE RESULTS ARE : {0}", result.Result.ReadToEnd());
            Console.WriteLine("\n press any key to continue. ");
            Console.ReadKey();
        }

    }
}

public class MyHiveDatabase : HiveConnection
{
    //WebHcat client librabry manages the scheduling and execution of jobs in an HDInsight cluster
    //WebHDFS client library works with files in HDFS and Windows Azure Blob Storage 1.Scalable rest api ,move files in and out and delete from hdfs ,perform file and directory functions
    public MyHiveDatabase(Uri webHCatUri, string username, string password, string storageaccount, string storagekey) : base(webHCatUri, username, password, storageaccount, storagekey) { }

}
