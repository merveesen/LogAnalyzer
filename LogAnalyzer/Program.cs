// See https://aka.ms/new-console-template for more information
// See https://aka.ms/new-console-template for more information
using Newtonsoft.Json;
using System.Data;
using System.Net.Http.Json;
using System.Text;

//read log data
string[] rawLog = File.ReadAllLines("C:\\log.txt");

//get log info as a data table
DataTable parsedLogs = ParseRawLog(rawLog);

Console.WriteLine(GetLogReport(parsedLogs));

/// <summary>
/// returns log analysis as json string
/// </summary>
string GetLogReport(DataTable logTable)
{

    StringBuilder resultReport = new StringBuilder();

    //all possible methods
    List<string> methods = new List<string>() { "count_pending_messages", "get_messages", "get_friends_progress", "get_friends_score" };

    foreach (string method in methods)
    {
        int callCount = 0;
        decimal avgResponseTime = 0;
        decimal responseTimeMedian = 0;
        string mostRespondedDyno;

        //all log records for given method
        var logDetails = logTable.AsEnumerable().Where(x => x.Field<string>("path").Contains(method));

        if (logDetails.Count() == 0) //if method does not have any log skip
            continue;

        callCount = logDetails.Count();

        avgResponseTime = GetAvgResponseTime(method, logDetails);
        responseTimeMedian = GetMedianOfResponseTimes(method, logDetails);
        mostRespondedDyno = GetMostRespondedDyno(method, logDetails);

        resultReport.AppendLine(GenerateAnalysisReport(method, callCount, avgResponseTime, responseTimeMedian, mostRespondedDyno));
    }

    return resultReport.ToString();
}

/// <summary>
/// calculates and returns average response time for a given method
/// </summary>
decimal GetAvgResponseTime(string methodName, EnumerableRowCollection<DataRow> logDetails)
{

    int temp = 0;
    decimal connectTime = logDetails.AsEnumerable().Sum(x => int.TryParse(x.Field<string>("connect"), out temp) ? temp : 0);
    decimal serviceTime = logDetails.AsEnumerable().Sum(x => int.TryParse(x.Field<string>("service"), out temp) ? temp : 0);

    decimal result = (connectTime + serviceTime) / logDetails.Count();
    return result;

}

/// <summary>
/// calculates and returns median of response times
/// </summary>
decimal GetMedianOfResponseTimes(string methodName, EnumerableRowCollection<DataRow> logDetails)
{

    List<decimal> responseTimes = new List<decimal>();
    foreach (DataRow log in logDetails)
    {
        decimal connectTime = Convert.ToInt32(log.Field<string>("connect"));
        decimal serviceTime = Convert.ToInt32(log.Field<string>("service"));

        responseTimes.Add(connectTime + serviceTime);
    }

    responseTimes.Sort();
    if (responseTimes.Count % 2 == 1) //if odd then the median is the middle
        return responseTimes.ElementAt(responseTimes.Count / 2);
    else
        return (responseTimes.ElementAt(responseTimes.Count / 2) + responseTimes.ElementAt((responseTimes.Count / 2) - 1)) / 2M;
}

string GetMostRespondedDyno(string methodName, EnumerableRowCollection<DataRow> logDetails)
{
    return logDetails.GroupBy(x => x.Field<string>("dyno"))
            .OrderByDescending(g => g.Count())
            .Select(g => g.Key)
            .First()
            .ToString();
}

/// <summary>
/// converts raw log rows into a data table
/// </summary>
DataTable ParseRawLog(string[] rawLog)
{
    DataTable dt = new DataTable();
    bool columnsAreCreated = false;

    foreach (string row in rawLog)
    {
        if (row.Length == 0) //if row is empty, skip
            continue;

        DataRow dr = dt.NewRow();
        foreach (string item in row.Split(' ').Skip(2))
        {
            string[] cell = item.Split("=");
            if (!columnsAreCreated) // create columns only for the first iteration
            {
                DataColumn column = new DataColumn(cell[0]); //columnname is leftside of the "="
                dt.Columns.Add(column);
            }

            if (cell[0] == "connect" || cell[0] == "service")
            {
                cell[1] = cell[1].Replace("ms", string.Empty);
            }

            dr[cell[0]] = cell.Length > 1 ? cell[1] : string.Empty; //cell value
        }
        columnsAreCreated = true;

        dt.Rows.Add(dr);
    }

    return dt;
}

string GenerateAnalysisReport(string methodName, int callCount, decimal mean, decimal median, string dynoMode)
{
    var jsonReport = new
    {
        request_identifier = "GET /api/users/{user_id}/" + methodName,
        called = callCount,
        response_time_mean = mean,
        response_time_median = median,
        dyno_mode = dynoMode
    };

    return JsonConvert.SerializeObject(jsonReport);
}
