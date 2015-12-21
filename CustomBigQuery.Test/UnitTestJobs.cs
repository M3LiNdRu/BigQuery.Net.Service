using Custom.BigQuery.Service;
using Google.Apis.Bigquery.v2;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CustomBigQuery.Test
{
    [TestClass]
    public class UnitTestJobs
    {
        private AuthService auth = new AuthService();

        [TestMethod]
        public void TestMethodInsertAll()
        {

            BigqueryService bq = auth.getServiceAuthP12();
            JobService job = new JobService(bq, "api-project-109606543851");
            List<GsodModel> gsodData = new List<GsodModel>() {
                new GsodModel() {station_number = 99, wban_number = 120, year = 2015 },
                new GsodModel() {station_number = 99, wban_number = 120, year = 2015 },
                new GsodModel() {station_number = 99, wban_number = 120, year = 2015 },
                new GsodModel() {station_number = 99, wban_number = 120, year = 2015 },
            };
            job.InsertAll<GsodModel>(gsodData,"prova","test");
            Debug.WriteLine("Hola");
            Assert.IsNotNull(bq);
        }
    }
}
