using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Custom.BigQuery.Service;
using Google.Apis.Bigquery.v2;

namespace CustomBigQuery.Test
{
    [TestClass]
    public class UnitTestAuth
    {
        private AuthService auth = new AuthService();

        [TestMethod]
        public void TestMethodClientAuth()
        {
            BigqueryService bq = auth.getAuth();

            Assert.IsNotNull(bq);
        }

        [TestMethod]
        public void TestMethodServiceAuth() {

            BigqueryService bq = auth.getServiceAuthP12();

            Assert.IsNotNull(bq);

        }
    }
}
