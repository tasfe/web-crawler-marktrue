using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Net;
using System.IO;

namespace WebCrawler.Core
{
    class SearchCrawler
    {
        private string m_szStartUrl;
        private int m_nMaxProcessdUrl;
        //private string m_sz
    }

    public struct UrlElem
    {
        public string szUrl;
        public string szTitle;

        public UrlElem(string title, string url)
        {
            szTitle = title;
            szUrl = url;
        }
    }

    public class SearchCrawlerWorker
    {

        private static Queue<string> s_oURLQue = new Queue<string>(c_nMaxQueSize);

        private static Semaphore s_oDeQueSema = new Semaphore(0, c_nMaxQueSize);

        private static Semaphore s_oEnQueSema = new Semaphore(c_nMaxQueSize, c_nMaxQueSize);

        private static HashSet<string> s_oURLSet = new HashSet<string>();

        private static List<UrlElem> s_oUrlElemList = new List<UrlElem>();

        public static List<UrlElem> UrlElemList
        {
            get
            {
                return s_oUrlElemList;
            }
        }

        private int m_cntRunningThreads;

        public const int c_nMaxQueSize = 0x10000;

        public const int c_nMaxWorker = 1024;

        public SearchCrawlerWorker(string szTitle, string szStartUrl)
        {
            m_cntRunningThreads = 1;
            Init(szTitle, szStartUrl);
        }

        public SearchCrawlerWorker(string szTitle, string szStartUrl, int nWorker)
        {
            if (nWorker <= 0)
            {
                nWorker = 1;
            }
            if (nWorker > c_nMaxWorker)
            {
                nWorker = c_nMaxWorker;
            }
            m_cntRunningThreads = nWorker;
            Init(szTitle, szStartUrl);
        }

        public void Run()
        {
            string szContent;
            while (true)
            {
                // get a szURL from queue
                --m_cntRunningThreads;
                s_oDeQueSema.WaitOne();
                string szUrl = s_oURLQue.Dequeue();
                s_oEnQueSema.Release();
                ++m_cntRunningThreads;

                // working
                // download page
                szContent = DownLoadPage(szUrl);

                // invilid page
                if (szContent.Length < 1)
                {
                    continue;
                }

                // analyze and enqueue the url
                AnalyzeContent(szUrl, szContent);
            }
        }

        private void Init(string szTitle, string szStartUrl)
        {
            int i;
            int Threadnum = m_cntRunningThreads;
            Thread[] Threads = new Thread[Threadnum];
            s_oEnQueSema.WaitOne();
            s_oURLQue.Enqueue(szStartUrl);
            s_oDeQueSema.Release();
            for (i = 0; i < Threadnum; ++i)
            {
                Threads[i] = new Thread(new ThreadStart(this.Run));
                Threads[i].Start();
            }
            i = 0;
            while (true)
            {
                for (int j = 0;i < s_oUrlElemList.Count && j < 50; ++i, ++j)
                {
                    Console.WriteLine("title: {0}, url: {1}", s_oUrlElemList[i].szTitle, s_oUrlElemList[i].szUrl);
                }
                Thread.Sleep(5000);
            }
        }

        private string DownLoadPage(string szUrl)
        {
            HttpWebRequest oWebReq = null;
            HttpWebResponse oWebResp = null;
            StreamReader oWebReadStm = null;

            char[] cbuffer = new char[1024];
            string szRetBuffer = "";
            Uri oUri = new Uri(szUrl);

            try
            {
                oWebReq = (HttpWebRequest)WebRequest.Create(oUri);
                oWebResp = (HttpWebResponse)oWebReq.GetResponse();
                oWebReadStm = new StreamReader(oWebResp.GetResponseStream(), Encoding.Default);
                int nByteRead = oWebReadStm.Read(cbuffer, 0, 1024);

                while (nByteRead != 0)
                {
                    szRetBuffer = szRetBuffer.Insert(szRetBuffer.Length, new string(cbuffer, 0, nByteRead));
                    nByteRead = oWebReadStm.Read(cbuffer, 0, 1024);
                }

            }
            catch (Exception e)
            {
                ;
            }
            finally
            {
                if (oWebReadStm != null)
                {
                    oWebReadStm.Close();
                }
                if (oWebResp != null)
                {
                    oWebResp.Close();
                }
            }
            return szRetBuffer;
        }

        private void AnalyzeContent(string szPageUrl, string szContent)
        {
            string title;
            string link;
            Uri oUri = new Uri(szPageUrl);
            Regex oLinkReg = new Regex("<a\\s+href\\s*=\\s*\"?(.*?)[\"|>]", RegexOptions.IgnoreCase | RegexOptions.Compiled);
            Regex oTitleReg = new Regex("\\<title\\>.*\\</title\\>", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            MatchCollection oMatchCol = oLinkReg.Matches(szContent);
            Match oMatchTitle = oTitleReg.Match(szContent);
            title = oMatchTitle.Groups[0].Value;
            if (title != "")
            {
                title = title.Split(new char[] { '<', '>' })[2];
            }
            //
            s_oUrlElemList.Add(new UrlElem(title, szPageUrl));
            //
            foreach (Match omatch in oMatchCol)
            {
                link = omatch.Groups[1].Value;
                link = link.Trim();

                if (link.Length < 1)
                {
                    continue;
                }

                // inner link
                if (link.StartsWith("#"))
                {
                    continue;
                }

                // Email Address
                if (link.IndexOf("mailto:") != -1)
                {
                    continue;
                }

                // to be varified
                if (link.ToLower().IndexOf("javascript") != -1)
                {
                    continue;
                }

                if (link.IndexOf("://") == -1)
                {
                    if (link.StartsWith("/"))
                    {
                        // 转换成绝对路径
                        link = "http://" + oUri.Host + ":"
                          + oUri.Port + link;
                    }
                    else
                    {
                        String file = oUri.AbsoluteUri;
                        if (file.IndexOf('/') == -1)
                        {
                            // 处理相对地址
                            link = "http://" + oUri.Host + ":"
                              + oUri.Port + "/" + link;
                        }
                        else
                        {
                            String path = file.Substring(0,
                              file.LastIndexOf('/') + 1);
                            link = "http://" + oUri.Host + ":"
                              + oUri.Port + path + link;
                        }
                    }
                }

                int index;
                if ((index = link.IndexOf('#')) != -1)
                {
                    link = link.Substring(0, index);
                }

                if ((link = FormatURL(link)) == null)
                {
                    continue;
                }

                if (s_oURLSet.Contains(link))
                {
                    continue;
                }
                s_oURLSet.Add(link);

                // EnQueue
                s_oEnQueSema.WaitOne();
                s_oURLQue.Enqueue(link);
                s_oDeQueSema.Release();
            }
        }

        private string FormatURL(string szUrl)
        {
            szUrl = szUrl.ToLower();
            if (!szUrl.StartsWith("http://"))
            {
                return null;
            }
            if (!Uri.IsWellFormedUriString(szUrl, UriKind.Absolute))
            {
                return null;
            }
            return szUrl;
        }

    }
}
