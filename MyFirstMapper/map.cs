using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Hadoop.MapReduce;





namespace MyFirstMapper
{
    class map
    {
        static void Main(string[] args)
        {

            if (args.Length > 0)
            {
                Console.SetIn(new StreamReader(args[0]));
            }
            string line;
            // read the file line by line and create key
            while ((line = Console.ReadLine()) != null)
            {
                try
                {
                    var words = line.Split(',');
                    var key = "";
                    if (Convert.ToInt16(words[7]) <= 1)
                    {
                        key = "0-4yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 2)
                    {
                        key = "5-9yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 3)
                    {
                        key = "10-14yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 4)
                    {
                        key = "15-19yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 5)
                    {
                        key = "20-24yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 6)
                    {
                        key = "25-29yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 7)
                    {
                        key = "30-34yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 8)
                    {
                        key = "35-39yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 9)
                    {
                        key = "40-44yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 10)
                    {
                        key = "45-49yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 11)
                    {
                        key = "50-54yrs";
                    }
                    if (Convert.ToInt16(words[7]) <= 12)
                    {
                        key = "Above59yrs";
                    }
                    //define key value pair
                    Console.WriteLine(words[3] + "," + words[4] + "," + key + "\t" + words[9]);
                }
                catch (Exception msg)
                {
                }
            }
        
    }
}
}
