using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace AlertMediaUpdate
{
    public class ALERT_MEDIA_STRUCT
    {
        public string firstname;
        public string lastname;
        public string student; //yes or no
    }

    class Program
    {
        static bool DoMatch(ref List<string> list_firstname, ref List<string> list_lastname, ref List<bool> list_found, string searchfirstname, string searchlastname)
        {
            for (int ii = 0; ii < list_lastname.Count; ii++)
            {
                if (list_lastname[ii].Equals(searchlastname) || list_lastname[ii].Contains(searchlastname))
                {
                    if (list_firstname[ii].Equals(searchfirstname) || list_firstname[ii].StartsWith(searchfirstname))
                    {
                        list_found[ii] = true;
                        return true;
                    }
                }
            }
            return false;
        }


        static List<string> GetFirstnamesWhereLastnameIsOrHasDash(ref List<string> list_firstname, ref List<string> list_lastname, string searchlastname)
        {
            List<string> results = new List<string>();

            for (int ii = 0; ii < list_lastname.Count; ii++)
            {
                if (list_lastname[ii].Equals(searchlastname))
                    results.Add(list_firstname[ii]);
                else if (list_lastname[ii].StartsWith(searchlastname))
                {
                    //ok check if there is a dash after, for example: Hernandez-Castro
                    string tempstr = list_lastname[ii].Replace(searchlastname, "");
                    if (tempstr[0] == '-')
                        results.Add(list_firstname[ii]);

                }
            }

            return results;
        }

        static int DoQuery_Ultiproid(ref StreamWriter out_writer, ref SqlConnection conn, string ultiproid)
        {

            string ultiproid_query_str = string.Format("SELECT distinct PEOPLE.PEOPLE_CODE_ID as \"customer_user_id\", PEOPLE.FIRST_NAME as \"first_name\", PEOPLE.LAST_NAME as \"last_name\", isnull(ad.EMAIL_ADDRESS, '') as \"email\", '' as title, isnull(ad.alternate_fax, '') as \"mobile_phone\", isnull(dbo.fn_phonenumber(PEOPLE.personid, 'CELL'), '') as mobile_phone2, isnull(pp.phonenumber, '') as mobile_phone3, isnull(dbo.fn_phonenumber(PEOPLE.personid, 'DAY'), '') as \"home_phone\", isnull(dbo.fn_phonenumber(PEOPLE.personid, 'OFFICE'), '') as \"office_phone\", '' as \"office_phone_post_dial\", isnull(u.secondary_email, '') as \"email2\", '' as \"address\", '' as \"address2\", '' as \"city\", '' as \"state\", '' as \"zipcode\", '' as \"country\", '' as \"custom1\",  '' as \"custom2\", '' as \"custom3\", pu.UserName as \"notes\", 'no' as \"8388~Admins\", CASE WHEN p2.PEOPLE_TYPE = ('SD') THEN 'no' ELSE 'yes' END as \"9450~Burbank\", CASE WHEN pt.PEOPLE_TYPE in ('DEAN', 'CHR') THEN 'yes' ELSE 'no' END as \"9452~Faculty\", CASE WHEN p2.PEOPLE_TYPE = 'SD' THEN 'yes' ELSE 'no' END as \"9451~SanDiego\", 'yes' as \"9453~Staff\", 'no' as \"9454~Student\", '' as \"9449~StudentLeader1617\"  FROM  PEOPLE  INNER JOIN ADDRESSSCHEDULE ad ON PEOPLE.PEOPLE_CODE_ID = ad.PEOPLE_ORG_CODE_ID AND PEOPLE.PREFERRED_ADD = ad.ADDRESS_TYPE AND(ad.STATUS = 'a')  INNER JOIN PEOPLETYPE pt  ON PEOPLE.PEOPLE_ID = pt.PEOPLE_ID  left outer join personphone pp  on PEOPLE.primaryphoneid = pp.personphoneid  left outer join PEOPLETYPE p2  ON PEOPLE.PEOPLE_ID = p2.PEOPLE_ID and p2.PEOPLE_TYPE = 'SD' left outer join USERDEFINEDIND u  ON people.PEOPLE_CODE_ID = u.PEOPLE_CODE_ID  left outer join personuser pu  ON people.PersonId = pu.PersonId  WHERE  u.ULTIPROID = '{0}'", ultiproid);


            SqlCommand command = new SqlCommand(ultiproid_query_str, conn);
            SqlDataReader dr = command.ExecuteReader();

            int resultcount = 0;
            while (dr.Read())
            {
                resultcount++;

                string column_values = "";
                string temp;

                for (int ii = 0; ii < dr.FieldCount; ii++)
                {
                    //string myname = dr.GetName(ii);

                    Type mytype = dr.GetFieldType(ii);
                    if (mytype == typeof(System.String))
                    {
                        try
                        {
                            temp = dr.GetString(ii);
                        }
                        catch (SqlNullValueException ex)
                        {
                            temp = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (temp.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", temp);
                        }
                        else
                        {
                            if (temp.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", temp);
                        }
                    }
                    else if (mytype == typeof(System.DateTime))
                    {
                        string customstr;
                        DateTime mydatetime;
                        try
                        {
                            mydatetime = dr.GetDateTime(ii);
                            customstr = mydatetime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Int32))
                    {
                        string customstr;
                        Int32 myint;
                        try
                        {
                            myint = dr.GetInt32(ii);
                            customstr = myint.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Decimal))
                    {
                        string customstr;
                        Decimal mydec;
                        try
                        {
                            mydec = dr.GetDecimal(ii);
                            customstr = mydec.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Boolean))
                    {
                        string customstr;
                        bool mybool;
                        try
                        {
                            mybool = dr.GetBoolean(ii);
                        }
                        catch (SqlNullValueException ex)
                        {
                            mybool = false;
                        }

                        if (mybool)
                            customstr = "1";
                        else
                            customstr = "0";

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Int16))
                    {
                        string customstr;
                        Int16 myint;
                        try
                        {
                            myint = dr.GetInt16(ii);
                            customstr = myint.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else
                    {
                        throw new Exception("need to fix this query method");
                    }
                }
                out_writer.WriteLine(column_values);
            }
            dr.Close();

            return resultcount;
        }





        static int DoQuery_Ultiproid_NO_PREFERRED_ADDRESS(ref StreamWriter out_writer, ref SqlConnection conn, string ultiproid)
        {

            string ultiproid_query_str = string.Format("SELECT distinct PEOPLE.PEOPLE_CODE_ID as \"customer_user_id\", PEOPLE.FIRST_NAME as \"first_name\", PEOPLE.LAST_NAME as \"last_name\", isnull(ad.EMAIL_ADDRESS, '') as \"email\", '' as title, isnull(ad.alternate_fax, '') as \"mobile_phone\", isnull(dbo.fn_phonenumber(PEOPLE.personid, 'CELL'), '') as mobile_phone2, isnull(pp.phonenumber, '') as mobile_phone3, isnull(dbo.fn_phonenumber(PEOPLE.personid, 'DAY'), '') as \"home_phone\", isnull(dbo.fn_phonenumber(PEOPLE.personid, 'OFFICE'), '') as \"office_phone\", '' as \"office_phone_post_dial\", isnull(u.secondary_email, '') as \"email2\", '' as \"address\", '' as \"address2\", '' as \"city\", '' as \"state\", '' as \"zipcode\", '' as \"country\", '' as \"custom1\",  '' as \"custom2\", '' as \"custom3\", pu.UserName as \"notes\", 'no' as \"8388~Admins\", CASE WHEN p2.PEOPLE_TYPE = ('SD') THEN 'no' ELSE 'yes' END as \"9450~Burbank\", CASE WHEN pt.PEOPLE_TYPE in ('DEAN', 'CHR') THEN 'yes' ELSE 'no' END as \"9452~Faculty\", CASE WHEN p2.PEOPLE_TYPE = 'SD' THEN 'yes' ELSE 'no' END as \"9451~SanDiego\", 'yes' as \"9453~Staff\", 'no' as \"9454~Student\", '' as \"9449~StudentLeader1617\"  FROM  PEOPLE  INNER JOIN ADDRESSSCHEDULE ad ON PEOPLE.PEOPLE_CODE_ID = ad.PEOPLE_ORG_CODE_ID AND(ad.STATUS = 'a')  INNER JOIN PEOPLETYPE pt  ON PEOPLE.PEOPLE_ID = pt.PEOPLE_ID  left outer join personphone pp  on PEOPLE.primaryphoneid = pp.personphoneid  left outer join PEOPLETYPE p2  ON PEOPLE.PEOPLE_ID = p2.PEOPLE_ID and p2.PEOPLE_TYPE = 'SD' left outer join USERDEFINEDIND u  ON people.PEOPLE_CODE_ID = u.PEOPLE_CODE_ID  left outer join personuser pu  ON people.PersonId = pu.PersonId  WHERE  u.ULTIPROID = '{0}'", ultiproid);


            SqlCommand command = new SqlCommand(ultiproid_query_str, conn);
            SqlDataReader dr = command.ExecuteReader();

            int resultcount = 0;
            while (dr.Read())
            {
                resultcount++;

                string column_values = "";
                string temp;

                for (int ii = 0; ii < dr.FieldCount; ii++)
                {
                    //string myname = dr.GetName(ii);

                    Type mytype = dr.GetFieldType(ii);
                    if (mytype == typeof(System.String))
                    {
                        try
                        {
                            temp = dr.GetString(ii);
                        }
                        catch (SqlNullValueException ex)
                        {
                            temp = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (temp.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", temp);
                        }
                        else
                        {
                            if (temp.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", temp);
                        }
                    }
                    else if (mytype == typeof(System.DateTime))
                    {
                        string customstr;
                        DateTime mydatetime;
                        try
                        {
                            mydatetime = dr.GetDateTime(ii);
                            customstr = mydatetime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Int32))
                    {
                        string customstr;
                        Int32 myint;
                        try
                        {
                            myint = dr.GetInt32(ii);
                            customstr = myint.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Decimal))
                    {
                        string customstr;
                        Decimal mydec;
                        try
                        {
                            mydec = dr.GetDecimal(ii);
                            customstr = mydec.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Boolean))
                    {
                        string customstr;
                        bool mybool;
                        try
                        {
                            mybool = dr.GetBoolean(ii);
                        }
                        catch (SqlNullValueException ex)
                        {
                            mybool = false;
                        }

                        if (mybool)
                            customstr = "1";
                        else
                            customstr = "0";

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Int16))
                    {
                        string customstr;
                        Int16 myint;
                        try
                        {
                            myint = dr.GetInt16(ii);
                            customstr = myint.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else
                    {
                        throw new Exception("need to fix this query method");
                    }
                }
                out_writer.WriteLine(column_values);
            }
            dr.Close();

            return resultcount;
        }




        static int DoQuery_Ultiproid_NO_ADDRESS_AT_ALL(ref StreamWriter out_writer, ref SqlConnection conn, string ultiproid)
        {

            string ultiproid_query_str = string.Format("SELECT distinct PEOPLE.PEOPLE_CODE_ID as \"customer_user_id\", PEOPLE.FIRST_NAME as \"first_name\", PEOPLE.LAST_NAME as \"last_name\", '' as \"email\", '' as title, '' as \"mobile_phone\", isnull(dbo.fn_phonenumber(PEOPLE.personid, 'CELL'), '') as mobile_phone2, isnull(pp.phonenumber, '') as mobile_phone3, isnull(dbo.fn_phonenumber(PEOPLE.personid, 'DAY'), '') as \"home_phone\", isnull(dbo.fn_phonenumber(PEOPLE.personid, 'OFFICE'), '') as \"office_phone\", '' as \"office_phone_post_dial\", isnull(u.secondary_email, '') as \"email2\", '' as \"address\", '' as \"address2\", '' as \"city\", '' as \"state\", '' as \"zipcode\", '' as \"country\", '' as \"custom1\",  '' as \"custom2\", '' as \"custom3\", pu.UserName as \"notes\", 'no' as \"8388~Admins\", CASE WHEN p2.PEOPLE_TYPE = ('SD') THEN 'no' ELSE 'yes' END as \"9450~Burbank\", CASE WHEN pt.PEOPLE_TYPE in ('DEAN', 'CHR') THEN 'yes' ELSE 'no' END as \"9452~Faculty\", CASE WHEN p2.PEOPLE_TYPE = 'SD' THEN 'yes' ELSE 'no' END as \"9451~SanDiego\", 'yes' as \"9453~Staff\", 'no' as \"9454~Student\", '' as \"9449~StudentLeader1617\"  FROM  PEOPLE   INNER JOIN PEOPLETYPE pt  ON PEOPLE.PEOPLE_ID = pt.PEOPLE_ID  left outer join personphone pp  on PEOPLE.primaryphoneid = pp.personphoneid  left outer join PEOPLETYPE p2  ON PEOPLE.PEOPLE_ID = p2.PEOPLE_ID and p2.PEOPLE_TYPE = 'SD' left outer join USERDEFINEDIND u  ON people.PEOPLE_CODE_ID = u.PEOPLE_CODE_ID  left outer join personuser pu  ON people.PersonId = pu.PersonId  WHERE  u.ULTIPROID = '{0}'", ultiproid);


            SqlCommand command = new SqlCommand(ultiproid_query_str, conn);
            SqlDataReader dr = command.ExecuteReader();

            int resultcount = 0;
            while (dr.Read())
            {
                resultcount++;

                string column_values = "";
                string temp;

                for (int ii = 0; ii < dr.FieldCount; ii++)
                {
                    //string myname = dr.GetName(ii);

                    Type mytype = dr.GetFieldType(ii);
                    if (mytype == typeof(System.String))
                    {
                        try
                        {
                            temp = dr.GetString(ii);
                        }
                        catch (SqlNullValueException ex)
                        {
                            temp = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (temp.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", temp);
                        }
                        else
                        {
                            if (temp.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", temp);
                        }
                    }
                    else if (mytype == typeof(System.DateTime))
                    {
                        string customstr;
                        DateTime mydatetime;
                        try
                        {
                            mydatetime = dr.GetDateTime(ii);
                            customstr = mydatetime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Int32))
                    {
                        string customstr;
                        Int32 myint;
                        try
                        {
                            myint = dr.GetInt32(ii);
                            customstr = myint.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Decimal))
                    {
                        string customstr;
                        Decimal mydec;
                        try
                        {
                            mydec = dr.GetDecimal(ii);
                            customstr = mydec.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Boolean))
                    {
                        string customstr;
                        bool mybool;
                        try
                        {
                            mybool = dr.GetBoolean(ii);
                        }
                        catch (SqlNullValueException ex)
                        {
                            mybool = false;
                        }

                        if (mybool)
                            customstr = "1";
                        else
                            customstr = "0";

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else if (mytype == typeof(System.Int16))
                    {
                        string customstr;
                        Int16 myint;
                        try
                        {
                            myint = dr.GetInt16(ii);
                            customstr = myint.ToString();
                        }
                        catch (SqlNullValueException ex)
                        {
                            customstr = "NULL";
                        }

                        if (ii == (dr.FieldCount - 1))
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL";
                            else
                                column_values += string.Format("{0}", customstr);
                        }
                        else
                        {
                            if (customstr.Equals("NULL"))
                                column_values += "NULL,";
                            else
                                column_values += string.Format("{0},", customstr);
                        }
                    }
                    else
                    {
                        throw new Exception("need to fix this query method");
                    }
                }
                out_writer.WriteLine(column_values);
            }
            dr.Close();

            return resultcount;
        }




        static void Main(string[] args)
        {
            /*****************************************************************************
            string adjunct_csv = "./Summer 2018 - Adjuncts Added to PowerCampus.csv";
            int adjunct_firstname_index = 0;
            int adjunct_lastname_index = 1;


            List<string> adjunct_firstname = new List<string>();
            List<string> adjunct_lastname  = new List<string>();
            StreamReader adjunct_rdr       = new StreamReader(adjunct_csv);
            string adjunct_line = adjunct_rdr.ReadLine();
            while (adjunct_line != null)
            {
                string[] substrings = adjunct_line.Split(comma_delimiter);

                string temp_firstname = substrings[adjunct_firstname_index].Replace("\"", "").Trim();
                string[] first_subs = temp_firstname.Split(space_delimiter); //sometimes they put space with middle initial or middle name , so chop it off
                string firstname = first_subs[0];

                string temp_lastname = substrings[adjunct_lastname_index].Replace("\"", "").Trim();
                string[] last_subs = temp_lastname.Split(space_delimiter);  //sometimes they put space with Jr or Sr, so chop it off
                string lastname;
                if (last_subs.Length == 1)
                    lastname = last_subs[0];
                else //there was a space
                {
                    if (last_subs.Length > 2) //for example "De La Cruz"
                    {
                        lastname = temp_lastname;
                    }
                    else
                    {
                        //ok only 1 space, check if after space is jr or sr
                        string teststr = last_subs[1].ToLower();
                        if (teststr.StartsWith("jr") || teststr.StartsWith("sr"))
                            lastname = last_subs[0];
                        else
                            lastname = string.Format("{0} {1}", last_subs[0], last_subs[1]);
                    }
                }
                adjunct_firstname.Add(firstname);
                adjunct_lastname.Add(lastname);
                adjunct_line = adjunct_rdr.ReadLine();
            }
            adjunct_rdr.Close();
            *****************************************************************************************************************/





            /**********************************************************************************************************************************************************
                        SELECT distinct PEOPLE.PEOPLE_CODE_ID as "customer_user_id", 
                            PEOPLE.FIRST_NAME as "first_name", 
                            PEOPLE.LAST_NAME as "last_name", 
                            isnull(ad.EMAIL_ADDRESS, '') as "email", 
                            '' as title, 
                            isnull(ad.alternate_fax, '') as "mobile_phone", 
                            isnull(dbo.fn_phonenumber(PEOPLE.personid, 'CELL'), '') as mobile_phone2, 
                            isnull(pp.phonenumber, '') as mobile_phone3, 
                            isnull(dbo.fn_phonenumber(PEOPLE.personid, 'DAY'), '') as "home_phone", 
                            isnull(dbo.fn_phonenumber(PEOPLE.personid, 'OFFICE'), '') as "office_phone", 
                            '' as "office_phone_post_dial", 
                            isnull(u.secondary_email, '') as "email2", 
                            '' as "address", 
                            '' as "address2", 
                            '' as "city", 
                            '' as "state", 
                            '' as "zipcode", 
                            '' as "country", 
                            '' as "custom1",  
                            '' as "custom2", 
                            '' as "custom3", 
                            pu.UserName as "notes", 
                            'no' as "8388~Admins", 
                            CASE WHEN p2.PEOPLE_TYPE = ('SD') THEN 'no' ELSE 'yes' END as "9450~Burbank",
                            CASE WHEN pt.PEOPLE_TYPE in ('DEAN', 'CHR') THEN 'yes' ELSE 'no' END as "9452~Faculty",
                            CASE WHEN p2.PEOPLE_TYPE = 'SD' THEN 'yes' ELSE 'no' END as "9451~SanDiego",
                            'yes' as "9453~Staff", 
                            'no' as "9454~Student", 
                            '' as "9449~StudentLeader1617"
            FROM PEOPLE
            INNER JOIN ADDRESSSCHEDULE ad ON PEOPLE.PEOPLE_CODE_ID = ad.PEOPLE_ORG_CODE_ID AND(ad.STATUS = 'a')-- AND PEOPLE.PREFERRED_ADD = ad.ADDRESS_TYPE
            INNER JOIN PEOPLETYPE pt  ON PEOPLE.PEOPLE_ID = pt.PEOPLE_ID
            left outer join personphone pp on PEOPLE.primaryphoneid = pp.personphoneid
            left outer join PEOPLETYPE p2 ON PEOPLE.PEOPLE_ID = p2.PEOPLE_ID and p2.PEOPLE_TYPE = 'SD'
            left outer join USERDEFINEDIND u ON people.PEOPLE_CODE_ID = u.PEOPLE_CODE_ID
            left outer join personuser pu ON people.PersonId = pu.PersonId
            WHERE PEOPLE.LAST_NAME = 'Dahan' AND PEOPLE.FIRST_NAME = 'Laila'


            select* FROM ADDRESSSCHEDULE WHERE PEOPLE_ORG_CODE_ID = 'P000085447'
            select* FROM PEOPLE WHERE PEOPLE_CODE_ID = 'P000085447'
            select* FROM PersonPhone WHERE PersonId = '81035'
            select* FROM PersonUser WHERE PersonId = '81035'
            **********************************************************************************************************************************************************/






            //*********************************************************************************************************************
            //alertmedia_csv - contains csv of rows with (firstname, lastname, student==yes or no) to check against ultipro_csv
            //this is query from tortoise svn C:\wu\query_templates_and_examples\AlertMedia\AlertMedia Master Rosters.sql
            //*********************************************************************************************************************

            string alertmedia_csv = "./alertmedia_query_results.csv";
            int alertmedia_firstname_index = 1;
            int alertmedia_lastname_index = 2;
            int alertmedia_9454_student_index = 28;
            int alertmedia_skip = 1;

            char space_delimiter = ' ';
            char comma_delimiter = ',';
            char tab_delimiter = '\t';

            //Enter alertmedia_csv into a List<ALERT_MEDIA_STRUCT>
            List<string> original_check = new List<string>();
            List<ALERT_MEDIA_STRUCT> check_list = new List<ALERT_MEDIA_STRUCT>();
            StreamReader check_rdr = new StreamReader(alertmedia_csv);

            string output_csv = "./alertmedia_final.csv";
            StreamWriter out_writer = new StreamWriter(output_csv);

            //skip header in checklist
            for (int ii = 0; ii < alertmedia_skip; ii++)
            {
                string header_line = check_rdr.ReadLine();
                out_writer.WriteLine(header_line);
            }

            //Ok.....inside alertmedia_csv, we have to strip doublequotes from index 0 == last name and index 1 == first name and trim spaces
            string check_line = check_rdr.ReadLine();
            while (check_line != null)
            {
                original_check.Add(check_line);
                string[] substrings = check_line.Split(comma_delimiter);

                string student = substrings[alertmedia_9454_student_index].Trim(); //yes or no

                string temp_firstname = substrings[alertmedia_firstname_index].Replace("\"", "").Trim();
                string firstname = temp_firstname.Replace(".", ""); //sometimes firstname is just an initial with a period, so chop off the period

                string lastname = substrings[alertmedia_lastname_index].Replace("\"", "").Trim();


                ALERT_MEDIA_STRUCT item = new ALERT_MEDIA_STRUCT();
                item.firstname = firstname;
                item.lastname = lastname;
                item.student = student;

                check_list.Add(item);

                check_line = check_rdr.ReadLine();
            }
            check_rdr.Close();


            //**************************************************************************************
            //Human Resources Ultiproid file
            //ultipro_csv - contains csv of rows with (firstname, lastname) of staff
            //this is from ultipro app which Human Resources uses and they give this file to Jacky and Jacky gives it to me
            //Enter ultipro_csv into 4 lists: (1) firstname (2) lastname (3) ultiproid (4) found
            //**************************************************************************************

            string ultipro_csv = "./Active Roster 7.30.18 for IT.txt";
            int ultipro_job_index = 1;
            int ultipro_ultiproid_index = 4;
            int ultipro_skip = 1;

            List<string> ultipro_firstname = new List<string>();
            List<string> ultipro_lastname = new List<string>();
            List<string> ultipro_ultiproid = new List<string>();
            List<bool> ultipro_found = new List<bool>();
            StreamReader ultipro_rdr = new StreamReader(ultipro_csv);

            //skip header in ultipro file
            for (int ii = 0; ii < ultipro_skip; ii++)
                ultipro_rdr.ReadLine();

            //only need to save non-students from ultipro file
            string ultipro_line = ultipro_rdr.ReadLine();
            while (ultipro_line != null)
            {
                string[] tab_strings = ultipro_line.Split(tab_delimiter);
                string job_str = tab_strings[ultipro_job_index].Trim().ToLower();
                string ultiproid = tab_strings[ultipro_ultiproid_index];
                if (job_str.Equals("student") == false)
                {
                    string[] substrings = tab_strings[0].Split(comma_delimiter);

                    string temp_firstname = substrings[1].Replace("\"", "").Trim();
                    string[] first_subs = temp_firstname.Split(space_delimiter); //sometimes in ultipro-file firstname they put space with middle initial or middle name , so chop it off
                    string firstname = first_subs[0];

                    string temp_lastname = substrings[0].Replace("\"", "").Trim();
                    string[] last_subs = temp_lastname.Split(space_delimiter); //sometimes in ultipro-file lastname they put space with Jr or Sr, so chop it off
                    string lastname;
                    if (last_subs.Length == 1)
                        lastname = last_subs[0];
                    else //there was a space
                    {
                        if (last_subs.Length > 2) //for example "De La Cruz"
                        {
                            lastname = temp_lastname;
                        }
                        else
                        {
                            //ok only 1 space, check if after space is jr or sr
                            string teststr = last_subs[1].ToLower();
                            if (teststr.StartsWith("jr") || teststr.StartsWith("sr"))
                                lastname = last_subs[0];
                            else
                                lastname = string.Format("{0} {1}", last_subs[0], last_subs[1]);
                        }
                    }

                    ultipro_firstname.Add(firstname);
                    ultipro_lastname.Add(lastname);
                    ultipro_ultiproid.Add(ultiproid);
                    ultipro_found.Add(false);
                }
                ultipro_line = ultipro_rdr.ReadLine();
            }
            ultipro_rdr.Close();

            //***************************************
            //Ok....now we do the checking logic
            //***************************************
            List<string> not_student_not_in_ultipro = new List<string>();
            for (int xx = 0; xx < check_list.Count; xx++)
            {
                ALERT_MEDIA_STRUCT check_item = check_list[xx];
                if (check_item.student.Equals("yes"))
                {
                    Console.WriteLine("(1) STUDENT, GO TO OUTPUT - {0} {1}", check_item.firstname, check_item.lastname);
                    out_writer.WriteLine(original_check[xx]);
                }
                else //faculty or staff
                {

                    //Example: adjust list has firstname="Mercedes" lastname="Nelson Coffman", checklist says Mercedes Coffman, we need to match on Coffman
                    //if (DoMatch(ref adjunct_firstname, ref adjunct_lastname, check_item.firstname, check_item.lastname))
                    //{
                    //    Console.WriteLine("(2) ADJUNCT MATCH, GO TO OUTPUT - {0} {1}", check_item.firstname, check_item.lastname);
                    //    out_writer.WriteLine(original_check[xx]);
                    //}


                    if (DoMatch(ref ultipro_firstname, ref ultipro_lastname, ref ultipro_found, check_item.firstname, check_item.lastname))
                    {
                        Console.WriteLine("(3) ROSTER MATCH, GO TO OUTPUT - {0} {1}", check_item.firstname, check_item.lastname);
                        out_writer.WriteLine(original_check[xx]);
                    }
                    else
                    {
                        Console.WriteLine("(4) NOT STUDENT AND NOT IN ROSTER GO TO SEPARATE FILE NOT_STUDENT_NOT_ROSTER.csv!!!!! - {0} {1}", check_item.firstname, check_item.lastname);
                        not_student_not_in_ultipro.Add(original_check[xx]);
                    }
                }
            }


            //************************************************************************************************************************************************************************************************
            //not_student_not_roster
            //************************************************************************************************************************************************************************************************
            StreamWriter not_student_not_in_ultipro_writer = new StreamWriter("./NOT_STUDENT_NOT_IN_ULTIPRO_INPUT_FILE.csv");
            for (int ii = 0; ii < not_student_not_in_ultipro.Count; ii++)
                not_student_not_in_ultipro_writer.WriteLine(not_student_not_in_ultipro[ii]);
            not_student_not_in_ultipro_writer.Close();


            //************************************************************************************************************************************************************************************************
            //(1) the ultipro_found people where FALSE means not picked up in C:\wu\query_templates_and_examples\AlertMedia\AlertMedia Master Rosters.sql query
            //(2) that means maybe only in ultipro and not in powercampus OR in powercampus
            //************************************************************************************************************************************************************************************************
            StreamWriter in_ultipro_no_powercampus_writer = new StreamWriter("./IN_ULTIPRO_CAN_NOT_FIND_IN_POWERCAMPUS_THIS_SHOULD_BE_EMPTY_FILE.csv");
            SqlConnection conn = new SqlConnection("Data Source=budb01;Initial Catalog=Campus8;Integrated Security=True");
            conn.Open();
            for (int ii = 0; ii < ultipro_found.Count; ii++)
            {
                if (ultipro_found[ii] == false)
                {
                    int resultcount_ultiproid = DoQuery_Ultiproid(ref out_writer, ref conn, ultipro_ultiproid[ii]);
                    if (resultcount_ultiproid == 0)
                    {
                        int resultcount2 = DoQuery_Ultiproid_NO_PREFERRED_ADDRESS(ref out_writer, ref conn, ultipro_ultiproid[ii]);
                        if (resultcount2 == 0)
                        {
                            int resultcount3 = DoQuery_Ultiproid_NO_ADDRESS_AT_ALL(ref out_writer, ref conn, ultipro_ultiproid[ii]);
                            if (resultcount3 == 0)
                                in_ultipro_no_powercampus_writer.WriteLine(string.Format("{0}, {1}, {2}", ultipro_ultiproid[ii], ultipro_firstname[ii], ultipro_lastname[ii]));
                        }
                    }
                }
            }
            out_writer.Close();
            in_ultipro_no_powercampus_writer.Close();


            Console.WriteLine("DONE");
        }
    }
}
