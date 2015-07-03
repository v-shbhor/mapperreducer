using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReactiveExtension
{

    class Program
    {
        class crime
        {
            public string state { get; set; }
            public string city { get; set; }
            public int population { get; set; }
            public int totalcriminalactivity { get; set; }
            public decimal crimepercentage { get; set; }
        }
        static void Main(string[] args)
        {
            var crimes = new ObservableCollection<crime>();
            var crimeschanges = Observable.FromEventPattern((EventHandler<NotifyCollectionChangedEventArgs> ev)
            => new NotifyCollectionChangedEventHandler(ev),
            ev => crimes.CollectionChanged += ev,
            ev => crimes.CollectionChanged -= ev);

            //performing the condition to watch mnost crime area - Here the cities with crime greater than 20% will be tracked

            var watchformostcrimes = from c in crimeschanges
                                     where c.EventArgs.Action == NotifyCollectionChangedAction.Add
                                     from crm in c.EventArgs.NewItems.Cast<crime>().ToObservable()
                                     where crm.crimepercentage >= 5
                                     select crm;

            //local path for inpuit text file
            string inputpath = @"C:\data\CityCrimeData.csv";

            //output path file
            string outputpath = @"C:\data\CityCrimeResultData.csv";

            Console.WriteLine("getting info from top crime areas");
            Console.WriteLine("State -> city -> population -> totalcriminal activity -> % crime");

            
            //Handler for the applied condition
            watchformostcrimes.Subscribe(crm =>
            {

                Console.WriteLine("Crime data :- {0} -> {1} -> {2} -> {3} -> {4}", crm.state, crm.city, crm.population, crm.totalcriminalactivity, crm.crimepercentage);
                System.IO.File.AppendAllText(outputpath, crm.state + ',' + crm.city + ',' + crm.population + ',' + crm.totalcriminalactivity + ',' + crm.crimepercentage + "\r\n");
            });

            if (System.IO.File.Exists(inputpath))
            {
                string[] allcrimes = System.IO.File.ReadAllLines(inputpath);
                foreach (string crmdata in allcrimes)
                {
                    string[] cmrpart = crmdata.Split(',');
                    try
                    {
                        //creating crime object
                        crime crimeobject = new crime()
                        {
                            state = cmrpart[0],
                            city = cmrpart[1],
                            population = Convert.ToInt32(cmrpart[2]),
                            totalcriminalactivity = Convert.ToInt32(cmrpart[3]),
                            crimepercentage = Convert.ToDecimal(cmrpart[4])
                        };
                        crimes.Add(crimeobject);
                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine("ERROR " + ex.Message);
                        break;
                    }
                }
            }
        }
    }
}


