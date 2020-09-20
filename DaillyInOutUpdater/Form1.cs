using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DaillyInOutUpdater
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
           
        }

        private void button1_Click(object sender, EventArgs e)
        {
            string text;
            var fileStream = new FileStream(@"c:\inout\file.txt", FileMode.Open, FileAccess.Read);
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8))
            {
                text = streamReader.ReadToEnd();
            }
            DateTime day = Convert.ToDateTime(dateTimePicker1.Text);
            //day = day.AddDays(-1);
            string filename = String.Format("{0:dd-MM-yyyy}", day);
            string Write_filepath = text + @":\AttendanceLogs_FB\FB_" + filename + ".csv";

            int sMonth = Convert.ToInt32(day.ToString("MM"));
            String Year = day.Year.ToString();
            string month_Year = sMonth + "_" + Year;

            string constr = ReadConString();
            DataTable dt = ReadFromSql(constr, month_Year,day);
            BulkCopy(dt, constr,day);
            ReadAndUpdateOutTimeFromSql(constr, month_Year,day);

            DataTable dtfb = ReadFromSqlFB(constr,day);
            CreateCSV(dtfb, Write_filepath);

           
        }
        private string ReadConString()
        {
            string text;
            var fileStream = new FileStream(@"c:\inout\ConString.txt", FileMode.Open, FileAccess.Read);
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8))
            {
                text = streamReader.ReadToEnd();
            }
            return text;
        }

        private DataTable ReadFromSql(string stConnectionString, string month_Year,DateTime date)
        {
            DataSet ds = new DataSet();
            try
            {
                SqlConnection sqlCon = new SqlConnection(stConnectionString);
                string queryString = " select distinct [UserId] [Employee Code],CONVERT(VARCHAR(12),cast('" + date + "' as date),103) [Attendance Date],MIN(CONVERT(VARCHAR(12),[LogDate],108)) [In Time] "
                                        + ",MAX(CONVERT(VARCHAR(12),[LogDate],108)) [Out Time] "
                                        + "FROM [DeviceLogs_" + month_Year + "] where CONVERT(VARCHAR(12),[LogDate],103)=CONVERT(VARCHAR(12),cast('" + date + "' as date),103) "
                                        + " and CONVERT(VARCHAR(12),[LogDate],108) between  '08:00:00' and '24:00:00'"
                                        + "group by  [UserId] order by [UserId]";
                using (SqlConnection connection = sqlCon)
                {
                    SqlDataAdapter adapter = new SqlDataAdapter();
                    adapter.SelectCommand = new SqlCommand(queryString, connection);
                    adapter.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString(), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            return ds.Tables[0];
        }

        private void ReadAndUpdateOutTimeFromSql(string stConnectionString, string month_Year, DateTime date)
        {
            DataSet ds = new DataSet();
            try
            {
                //  string stConnectionString = "data source=34.202.152.47;initial catalog=InOutDB;user id=sa;pwd=Bk@123456;Connect Timeout=0";
                //string stConnectionString = "data source=localhost;initial catalog=eSSLSmartOffice;user id=sa;pwd=tst;Connect Timeout=0";
                SqlConnection sqlCon = new SqlConnection(stConnectionString);
                string queryString = " select distinct [UserId] [Employee Code],CONVERT(VARCHAR(12),(DATEADD (day , 1 , (cast('5/26/2017 12:00:00 AM' as date)) )),103) [Attendance Date],MIN(CONVERT(VARCHAR(12),[LogDate],108)) [In Time] "
                                        + ",MAX(CONVERT(VARCHAR(12),[LogDate],108)) [Out Time] "
                                        + "FROM [DeviceLogs_" + month_Year + "] where CONVERT(VARCHAR(12),[LogDate],103)=CONVERT(VARCHAR(12),(DATEADD (day , 1 , (cast('5/26/2017 12:00:00 AM' as date)) )),103) "
                                        + " and CONVERT(VARCHAR(12),[LogDate],108) between  '01:00:00' and '07:00:00'"
                                        + "group by  [UserId] order by [UserId]";
                using (SqlConnection connection = sqlCon)
                {
                    SqlDataAdapter adapter = new SqlDataAdapter();
                    adapter.SelectCommand = new SqlCommand(queryString, connection);
                    adapter.Fill(ds);
                }

                if (ds != null)
                {
                    DataTable dt = new DataTable();
                    dt = ds.Tables[0];
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                       // string stConnectionStringfb = "data source=34.202.152.47;initial catalog=InOutDB;user id=sa;pwd=Bk@123456;Connect Timeout=0";

                        SqlConnection con = new SqlConnection(stConnectionString);
                        SqlCommand cmd = new SqlCommand();
                        cmd.Connection = con;
                        con.Open();
                        cmd.CommandText = "UPDATE [Inouttbl]  SET [Out Time] = '" + dt.Rows[i]["Out Time"].ToString() + "' WHERE [Attendance Date]= CONVERT(VARCHAR(12),cast('" + date + "' as date),103) and [Employee Code]= '" + dt.Rows[i]["Employee Code"].ToString() + "'";

                        //cmd.Parameters.AddWithValue("@OutTime", dt.Rows[i]["Out Time"].ToString());
                        //cmd.Parameters.AddWithValue("@AttendanceDate", dt.Rows[i]["Attendance Date"].ToString());
                        //cmd.Parameters.AddWithValue("@EmployeeCode", dt.Rows[i]["Employee Code"].ToString());

                        cmd.ExecuteNonQuery();
                        con.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString(), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }

        }

        private void BulkCopy(DataTable dt, string stConnectionString,DateTime date)
        {
            try
            {
                //string stConnectionString = "data source=34.202.152.47;initial catalog=InOutDB;user id=sa;pwd=Bk@123456;Connect Timeout=0";
                SqlConnection sqlCon = new SqlConnection(stConnectionString);

                string queryString = " Delete FROM [Inouttbl] WHERE [Attendance Date]= CONVERT(VARCHAR(12),cast('" + date + "' as date),103)";

                SqlConnection con = new SqlConnection(stConnectionString);
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = con;
                con.Open();
                cmd.CommandText = queryString;
                cmd.ExecuteNonQuery();
                con.Close();

                using (SqlConnection destinationCon = sqlCon)
                {
                    using (SqlBulkCopy bc = new SqlBulkCopy(destinationCon))
                    {
                        bc.DestinationTableName = "Inouttbl";
                        bc.ColumnMappings.Add("Attendance Date", "Attendance Date");
                        bc.ColumnMappings.Add("Employee Code", "Employee Code");
                        bc.ColumnMappings.Add("In Time", "In Time");
                        bc.ColumnMappings.Add("Out Time", "Out Time");
                        destinationCon.Open();
                        bc.WriteToServer(dt);
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString(), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private DataTable ReadFromSqlFB(string stConnectionString,DateTime date)
        {
            DataSet ds = new DataSet();
            try
            {
                //string stConnectionString = "data source=34.202.152.47;initial catalog=InOutDB;user id=sa;pwd=Bk@123456;Connect Timeout=0";
                SqlConnection sqlCon = new SqlConnection(stConnectionString);
                string queryString = " SELECT [Employee Code], [Attendance Date] ,[In Time],[Out Time]  FROM [Inouttbl] WHERE [Attendance Date]= CONVERT(VARCHAR(12),cast('" + date + "' as date),103)";
                using (SqlConnection connection = sqlCon)
                {
                    SqlDataAdapter adapter = new SqlDataAdapter();
                    adapter.SelectCommand = new SqlCommand(queryString, connection);
                    adapter.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString(), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
            return ds.Tables[0];
        }

        private void CreateCSV(DataTable dt, string path)
        {
            // string path = @"D:\AttendanceLogs_FB\" + filename;
            try
            {

                using (var w = new StreamWriter(path))
                {
                    if (dt.Rows.Count > 0)
                    {
                        for (int i = 0; i < dt.Rows.Count; i++)
                        {
                            var line = "";
                            if (i == 0)
                            {
                                line = string.Format("{0},{1},{2},{3}", "Employee Code", "Attendance Date", "In Time", "Out Time");
                                w.WriteLine(line);
                                w.Flush();
                            }

                            var first = dt.Rows[i]["Employee Code"].ToString();
                            var second = dt.Rows[i]["Attendance Date"].ToString();
                            var third = dt.Rows[i]["In Time"].ToString();
                            var four = dt.Rows[i]["Out Time"].ToString();

                            line = string.Format("{0},{1},{2},{3}", first, second, third, four);

                            w.WriteLine(line);
                            w.Flush();
                        }
                        label1.Text = "File On " + DateTime.Now + " Created Successfully.";
                    }
                    else
                    {
                        label1.Text = "No Data in File - " + DateTime.Now;
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString(), "Error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private void UploadFileToFTP(string filename, string Write_filepath)
        {
            try
            {

                FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://54.204.7.248/FB_" + filename + ".csv");
                request.Method = WebRequestMethods.Ftp.UploadFile;

                // This example assumes the FTP site uses anonymous logon.
                request.Credentials = new NetworkCredential("newgen", "newgen17122013");

                // Copy the contents of the file to the request stream.
                StreamReader sourceStream = new StreamReader(Write_filepath);
                byte[] fileContents = Encoding.UTF8.GetBytes(sourceStream.ReadToEnd());
                sourceStream.Close();
                request.ContentLength = fileContents.Length;

                Stream requestStream = request.GetRequestStream();
                requestStream.Write(fileContents, 0, fileContents.Length);
                requestStream.Close();

                FtpWebResponse response = (FtpWebResponse)request.GetResponse();

                label2.Text = "Upload File Complete, status " + response.StatusDescription;

                response.Close();
            }
            catch (Exception ex)
            {
                string msg = ex.ToString();
                label2.Text = ex.ToString();
            }
        }
    }
}
