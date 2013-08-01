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

    public class SearchCrawlerWorker
    {

        private static Queue<string> s_oURLQue = new Queue<string>(c_nMaxQueSize);

        private static Semaphore s_oDeQueSema = new Semaphore(0, c_nMaxQueSize);

        private static Semaphore s_oEnQueSema = new Semaphore(c_nMaxQueSize, c_nMaxQueSize);

        private static List<string> s_oURLList = new List<string>();

        private int m_cntRunningThreads;

        public const int c_nMaxQueSize = 0x10000;

        public const int c_nMaxWorker = 1024;

        public SearchCrawlerWorker(string szStartUrl)
        {
            m_cntRunningThreads = 1;
            Init(szStartUrl);
        }

        public SearchCrawlerWorker(string szStartUrl, int nWorker)
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
            Init(szStartUrl);
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
                // analyze and enqueue the url
                AnalyzeContent(szUrl, szContent);
            }
        }

        private void Init(string szStartUrl)
        {
            Thread[] Threads = new Thread[m_cntRunningThreads];
            s_oEnQueSema.WaitOne();
            s_oURLQue.Enqueue(szStartUrl);
            s_oDeQueSema.Release();
            for (int i = 0; i < m_cntRunningThreads; ++i)
            {
                Threads[i] = new Thread(new ThreadStart(this.Run));
                Threads[i].Start();
            }
        }

        private string DownLoadPage(string szUrl)
        {
            char[] cbuffer = new char[1024];
            string szRetBuffer = "";
            Uri oUri = new Uri(szUrl);
            HttpWebRequest oWebReq = (HttpWebRequest)WebRequest.Create(oUri);
            HttpWebResponse oWebResp = (HttpWebResponse)oWebReq.GetResponse();
            StreamReader oWebReadStm = new StreamReader(oWebResp.GetResponseStream());
            int nByteRead = oWebReadStm.Read(cbuffer, 0, 1024);

            while (nByteRead != 0)
            {
                szRetBuffer.Insert(szRetBuffer.Length, new string(cbuffer, 0, nByteRead));
                nByteRead = oWebReadStm.Read(cbuffer, 0, 1024);
            }

            oWebReadStm.Close();
            oWebResp.Close();
            return szRetBuffer;
        }

        private void AnalyzeContent(string szPageUrl, string szContent)
        {
            string link;
            Uri oUri = new Uri(szPageUrl);
            Regex oReg = new Regex("<a\\s+href\\s*=\\s*\"?(.*?)[\"|>]", RegexOptions.IgnoreCase | RegexOptions.Compiled);
            MatchCollection oMatchCol = oReg.Matches(szContent);
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

                if (s_oURLList.Contains(link))
                {
                    continue;
                }

                if ((link = FormatURL(link)) == null)
                {
                    continue;
                }
                s_oURLList.Add(link);

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
