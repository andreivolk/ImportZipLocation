using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using System.Data;
using System.Device.Location;

namespace ImportZipLocation
{
    class Program
    {
        private static List<ZipLocation> zipList = new List<ZipLocation>();
        private static List<BsonDocument> insertList = new List<BsonDocument>();
        private static List<ZipNeighbors> zipNeighborsList = new List<ZipNeighbors>();
        private static int radiusLimit = 16094; //How far you want to search for neighbors (in Meters)

        static void Main(string[] args)
        {
            ReadTSV();
            FindNeighbors();
            InsertRecords();
            Console.ReadLine();
        }

        private static void ReadTSV()
        {
            Console.WriteLine("Starting read...");
            try
            {
                DataTable table = new DataTable();
                table.Columns.Add("Country");
                table.Columns.Add("Zip");
                table.Columns.Add("City");
                table.Columns.Add("name1");
                table.Columns.Add("code1");
                table.Columns.Add("name2");
                table.Columns.Add("code2");
                table.Columns.Add("name3");
                table.Columns.Add("code3");
                table.Columns.Add("Latitude");
                table.Columns.Add("Longitude");
                table.Columns.Add("Accuracy");

                using (StreamReader sr = new StreamReader(ConfigurationManager.AppSettings["importTSV"]))
                {                   
                    while (sr.Peek() >= 0)
                    {
                        table.Rows.Add(sr.ReadLine().Split('\t'));
                    }
                    Console.WriteLine("Finished reading records");
                }
                foreach (DataRow row in table.Rows)
                {
                    ZipLocation newZip = new ZipLocation();
                    newZip.Zip = Int32.Parse(row.ItemArray[1].ToString());
                    newZip.Latitude = double.Parse(row.ItemArray[9].ToString());
                    newZip.Longtitude = double.Parse(row.ItemArray[10].ToString());
                    zipList.Add(newZip);
                }
                Console.WriteLine("Finished parsing records into list");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static void FindNeighbors()
        {
            Console.WriteLine("Finding neighbors...");
            try
            {
                foreach(var zipLoc in zipList)
                {
                    var newZipNeighbor = new ZipNeighbors();
                    newZipNeighbor.Zip = zipLoc.Zip;
                    var neighborList = new List<int>();
                    neighborList.Add(zipLoc.Zip);

                    foreach (var zip in zipList)
                    {
                        var startCoord = new GeoCoordinate(zipLoc.Latitude, zipLoc.Longtitude);
                        var neighborCoord = new GeoCoordinate(zip.Latitude, zip.Longtitude);
                        var distance = startCoord.GetDistanceTo(neighborCoord);

                        if(distance < radiusLimit)
                        {
                            neighborList.Add(zip.Zip);
                        }
                    }

                    newZipNeighbor.Neighbors = neighborList.ToArray();
                    zipNeighborsList.Add(newZipNeighbor);
                }
                Console.WriteLine("Finished building neighbor list");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static void InsertRecords()
        {
            Console.WriteLine("Converting list to BSON for insertion");
            try
            {
                var insertList = new List<BsonDocument>();
                foreach (var zip in zipNeighborsList)
                {
                    string zipJSON = JsonConvert.SerializeObject(zip);
                    BsonDocument zipInsert = BsonDocument.Parse(zipJSON);
                    insertList.Add(zipInsert);
                }
                Console.WriteLine("Inserting list");
                var client = new MongoClient(ConfigurationManager.AppSettings["dbUri"]);
                var db = client.GetDatabase(ConfigurationManager.AppSettings["database"]);
                var zips = db.GetCollection<BsonDocument>("ZipNeighbors");
                zips.InsertMany(insertList);
                Console.WriteLine("Inserting complete, press any key to exit");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}
