using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Hadoop.MapReduce;

namespace MyFirstReducer
{
    class reduce
    {
        static void Main(string[] args)
        {
            string line;

            if (args.Length > 0)
            {
                Console.SetIn(new StreamReader(args[0]));
            }

            //counter for each key
            var UriCounters = new Dictionary<string, int>();

            //list of uri ordered by the counter value
            var topUriList = new SortedList<int, string>();
            var count = 0;
            while ((line = Console.ReadLine()) != null)
            {
                try
                {
                    //parse the key and associated values
                    var words = line.Split('\t');
                    string key = words[0];
                    int value = int.Parse(words[1]);
                    //sum the values for each key in UriCounters
                    if (!UriCounters.ContainsKey(key))
                    {
                        count = value;
                        UriCounters.Add(key, value);
                    }
                    else
                        count += value;
                    UriCounters[key] = count;
                }
                catch (Exception ex)
                {
                }
            }
            foreach (var keyvalue in UriCounters)
                Console.WriteLine(string.Format("{0}", keyvalue.Key + "," + keyvalue.Value));
        }
    }
}


