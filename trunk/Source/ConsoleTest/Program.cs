using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using WebCrawler.Core;

namespace ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            SearchCrawlerWorker oSCWorker = new SearchCrawlerWorker("renren", "http://www.renren.com/", 20);
        }
    }
}
