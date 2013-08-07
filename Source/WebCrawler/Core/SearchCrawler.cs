using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Net;
using System.IO;
using System.Data.Sql;
using System.Data.SqlClient;

namespace WebCrawler.Core
{
    class SearchCrawlerDBConnection
    {

        private string m_szDBSrc = "1305LX\\SQL2008EXPRESS";

        private string m_szDBName = "CrawlerData";

        private string m_szUser = "sa";

        private string m_szPassword = "luxiao!234";

        private string m_szConStr;

        private SqlConnection oSqlCon;

        public SearchCrawlerDBConnection()
        {
            Init();
        }

        public SearchCrawlerDBConnection(string szDataSrc, string szName, string szUser, string szPwd)
        {
            m_szDBSrc = szDataSrc;
            m_szDBName = szName;
            m_szUser = szUser;
            m_szPassword = szPwd;
            Init();
        }

        private void Init()
        {
            m_szConStr = "Data Source=" + m_szDBSrc + 
                ";Database=" + m_szDBName + 
                ";Uid=" + m_szUser +
                ";Pwd=" + m_szPassword + ";";
            oSqlCon = new SqlConnection(m_szConStr);
        }

        public void Connect()
        {
            oSqlCon.Open();
        }

        public void Close()
        {
            oSqlCon.Close();
        }

        public void Write2DB(UrlElem oUrlElem)
        {
            SqlCommand oSqlChk = new SqlCommand("select * from UrlData where url = '" + oUrlElem.szUrl + "'", oSqlCon);
            SqlDataReader oDataReader = oSqlChk.ExecuteReader();
            bool k = oDataReader.Read();
            oDataReader.Close();
            if (k)
            {
                return;
            }
            SqlCommand oSqlCmd = new SqlCommand("insert UrlData(url,title,html) values('" + oUrlElem.szUrl + "','" + 
                oUrlElem.szTitle + "','" + " " + "')", oSqlCon);
                //oUrlElem.szTitle + "','" + oUrlElem.szText + "')", oSqlCon);
            oSqlCmd.ExecuteNonQuery();
        }

    }

    public struct UrlElem
    {
        public string szUrl;
        public string szTitle;
        public string szText;

        public UrlElem(string title, string url, string text)
        {
            szTitle = title;
            szUrl = url;
            szText = text;
        }
    }

    public class SearchCrawlerWorker
    {

        private static Queue<string> s_oURLQue = new Queue<string>(c_nMaxQueSize);

        private static Semaphore s_oElemDeQueSema = new Semaphore(0, c_nMaxQueSize);

        private static Semaphore s_oElemEnQueSema = new Semaphore(c_nMaxQueSize, c_nMaxQueSize);

        private static Semaphore s_oUrlDeQueSema = new Semaphore(0, c_nMaxQueSize);

        private static Semaphore s_oUrlEnQueSema = new Semaphore(c_nMaxQueSize, c_nMaxQueSize);

        private static HashSet<string> s_oURLSet = new HashSet<string>();

        private static Queue<UrlElem> s_oUrlElemQue = new Queue<UrlElem>(c_nMaxQueSize);

        private static List<UrlElem> s_oUrlElemList = new List<UrlElem>();

        private static int s_nMaxProcessdUrl = 0x10000;

        private int m_cntRunningThreads;

        public const int c_nMaxQueSize = 0x10000;

        public const int c_nMaxWorker = 0x400;

        public static List<UrlElem> UrlElemList
        {
            get
            {
                return s_oUrlElemList;
            }
        }

        public SearchCrawlerWorker(string szTitle, string szStartUrl)
        {
            m_cntRunningThreads = 1;
            Init(szTitle, szStartUrl);
        }

        public SearchCrawlerWorker(string szTitle, string szStartUrl, int nMaxProcessdUrl)
        {
            m_cntRunningThreads = 1;
            s_nMaxProcessdUrl = nMaxProcessdUrl;
            Init(szTitle, szStartUrl);
        }

        public SearchCrawlerWorker(string szTitle, string szStartUrl, int nMaxProcessdUrl, int nWorker)
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
            s_nMaxProcessdUrl = nMaxProcessdUrl;
            Init(szTitle, szStartUrl);
        }

        public void Crawl()
        {
            string szContent;
            while (true)
            {
                // get a szURL from queue
                --m_cntRunningThreads;
                s_oUrlDeQueSema.WaitOne();
                string szUrl = s_oURLQue.Dequeue();
                s_oUrlEnQueSema.Release();
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

        public void SaveData()
        {
            UrlElem oUrlElem;
            SearchCrawlerDBConnection oDBCon = new SearchCrawlerDBConnection();
            try
            {
                oDBCon.Connect();
                while (true)
                {
                    s_oElemDeQueSema.WaitOne();
                    oUrlElem = s_oUrlElemQue.Dequeue();
                    s_oElemEnQueSema.Release();
                    oDBCon.Write2DB(oUrlElem);
                }
            }
            catch (Exception e)
            {
                oDBCon.Close();
            }
        }

        private void Init(string szTitle, string szStartUrl)
        {
            int i;
            int Threadnum = m_cntRunningThreads;
            Thread[] Threads = new Thread[Threadnum];
            s_oUrlEnQueSema.WaitOne();
            s_oURLQue.Enqueue(szStartUrl);
            s_oUrlDeQueSema.Release();
            for (i = 0; i < Threadnum; ++i)
            {
                Threads[i] = new Thread(new ThreadStart(this.Crawl));
                Threads[i].Start();
            }
            Thread oDBWriteThd = new Thread(new ThreadStart(this.SaveData));
            oDBWriteThd.Start();
            i = 0;
            while (true)
            {
                /*
                for (int j = 0;i < s_oUrlElemQue.Count && j < 50; ++i, ++j)
                {
                    Console.WriteLine("title: {0}, url: {1}", s_oUrlElemQue[i].szTitle, s_oUrlElemQue[i].szUrl);
                }
                //*/
                if (m_cntRunningThreads <= 0)
                {
                    /*
                    for (; i < s_oUrlElemQue.Count; ++i)
                    {
                        Console.WriteLine("title: {0}, url: {1}", s_oUrlElemQue[i].szTitle, s_oUrlElemQue[i].szUrl);
                    }
                    //*/
                    for (i = 0; i < Threadnum; ++i)
                    {
                        Threads[i].Abort();
                    }
                    //while (oDBWriteThd.ThreadState != ThreadState.WaitSleepJoin)
                    //{
                        //Thread.Sleep(1000);
                    //}
                    //oDBWriteThd.Abort();
                    break;
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
                string[] tmp = oWebResp.ContentType.Split(new string[] { "charset=" }, StringSplitOptions.RemoveEmptyEntries);
                string szContentType = tmp[tmp.Length - 1];
                Encoding encode;
                if (szContentType.ToLower() == "text/html")
                {
                    encode = Encoding.Default;
                }
                else
                {
                    encode = Encoding.GetEncoding(szContentType);
                }
                oWebReadStm = new StreamReader(oWebResp.GetResponseStream(), encode);
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
            Regex oLinkReg = new Regex("<a(\\s+.+?\\s+|\\s+)href\\s*=\\s*\"?(.*?)[\"|>]", RegexOptions.IgnoreCase | RegexOptions.Compiled);
            Regex oTitleReg = new Regex("<title>((.|\\s)*?)</title>", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            MatchCollection oMatchCol = oLinkReg.Matches(szContent);
            Match oMatchTitle = oTitleReg.Match(szContent);
            title = oMatchTitle.Groups[1].Value;
            title = title.Trim();
            //
            UrlElem urlElem = new UrlElem(title, szPageUrl, szContent.Replace("\r\n", " ").Replace('\'','\"'));
            s_oElemEnQueSema.WaitOne();
            s_oUrlElemQue.Enqueue(urlElem);
            s_oElemDeQueSema.Release();
            //
            s_oUrlElemList.Add(urlElem);
            foreach (Match omatch in oMatchCol)
            {
                link = omatch.Groups[2].Value;
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
                            //link = "http://" + oUri.Host + ":"
                            // + oUri.Port + path + link;
                            link = path + link;
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
                s_oUrlEnQueSema.WaitOne();
                s_oURLQue.Enqueue(link);
                s_oUrlDeQueSema.Release();
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
