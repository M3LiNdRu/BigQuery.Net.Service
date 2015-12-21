using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Google.Apis.Bigquery.v2;
using Custom.BigQuery.Service;
using System.Diagnostics;

namespace CustomBigQuery.Test
{
    [TestClass]
    public class UnitTestTable
    {
        private AuthService auth = new AuthService();

        [TestMethod]
        public void TestMethodCreateTable()
        {
            BigqueryService bq = auth.getServiceAuthP12();
            TableService t = new TableService(bq, "api-project-109606543851");
            t.CreateTable(typeof(GsodModel), "test");
            Debug.WriteLine("Hello");
            Assert.IsNotNull(bq);
        }
    }
}
