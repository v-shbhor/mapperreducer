using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Hadoop.Hive;
using Microsoft.Hadoop.WebClient;

namespace LinQtoHive
{
    class Program
    {
        static void Main(string[] args)
        {
            //need to perform foll substitution to match ur env
            //replace {Azurestorageaccount} with root name of ur strage accnt
            var asvaccount = "shavas300.blob.core.windows.net";
            var asvkey = "W00+SmVFMQffNUikJpfAa8WOGdhka27fP42dnRG2FhuCxH4Mk7TaN2qGknqMwNjCXvbKCMwHeLJZWByhWiO4sQ==";
            var clustername = "shavas100";
            var clusterusername = "admin";
            var clusteruserpassword = "Ekdantay12!@";
            var clusteruri = "https://" + clustername + ".azurehdinsight.net"; //build cluster uri with full address

            //create hive connection
            Console.WriteLine("creating conn.......");
            var hiveconnection = new MyHiveDatabase(new System.Uri(clusteruri), clusterusername, clusteruserpassword, asvaccount,asvkey);

            Console.WriteLine("creating linq to hive query.......");

            //perform linq queries against Hive context

            var query = from census in hiveconnection.te_census_info1 where census.state == "California" select census;
            Console.WriteLine("running hive query...");
            query.ExecuteQuery().Wait();
            Console.WriteLine("getting hive query output...");
            var queryoutput = query.ToList(); //getting all census data into list var
            Console.WriteLine("total records in table..." + queryoutput.Count().ToString());
            Console.WriteLine("\n press any key to continue");
            Console.ReadKey();
        }
    }
}

    //Create hive database connection type as there is no automated generation support

    public class MyHiveDatabase : HiveConnection
    {
    //WebHcat client librabry manages the scheduling and execution of jobs in an HDInsight cluster
    //WebHDFS client library works with files in HDFS and Windows Azure Blob Storage 1.Scalable rest api ,move files in and out and delete from hdfs ,perform file and directory functions
        public MyHiveDatabase(Uri webHCatUri, string username, string password, string storageaccount, string storagekey) : base(webHCatUri, username, password, storageaccount, storagekey) { }

        public HiveTable<te_census_inforow> te_census_info1
        {
            get
            {
                //return the table on which you need to fire the query
                var hivetablename = "working_te_census_info1";
                return this.GetTable<te_census_inforow>(hivetablename);
            }
        }
    

    //define census data record structure . It should be the same as u defined while creating table in hive
    public class te_census_inforow : HiveRow
    {
        public string state { get; set; }
        public string country { get; set; }
        public string agegrp { get; set; }
        public string total_population { get; set; }
    }
}
    
    

