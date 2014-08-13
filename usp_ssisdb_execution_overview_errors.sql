USE [msdb]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[usp_ssisdb_execution_overview_errors] @execution_id int, @HTML_SSISDB nvarchar(max) output, @ssis_job_fail int output
AS
BEGIN

DECLARE @execution_overview_HTML nvarchar(max), @errors_HTML nvarchar(max), @ssis_execution_id_error_count int, @ssis_event_onerror_count int

--execution overview
set @execution_overview_HTML = N'<H4>SSIS Execution ID: ' + convert(varchar(255),@execution_id) + ' Overview </H4>'
	+ N'<table border = "1">'
	+ N'<th bgcolor = "#AFDACF">Status</th>'
	+ N'<th bgcolor = "#AFDACF">Execution ID</th>'
	+ N'<th bgcolor = "#AFDACF">Folder</th>'
	+ N'<th bgcolor = "#AFDACF">Project</th>'
	+ N'<th bgcolor = "#AFDACF">Package</th>'
	+ N'<th bgcolor = "#AFDACF">Environment</th>'
	+ N'<th bgcolor = "#AFDACF">Runtime</th>'
	+ N'<th bgcolor = "#AFDACF">Start</th>'
	+ N'<th bgcolor = "#AFDACF">End</th>'
	+ N'<th bgcolor = "#AFDACF">Duration</th>'
	+ N'<th bgcolor = "#AFDACF">Caller</th>'
	+ N'<th bgcolor = "#AFDACF">Server</th>'
	+ CAST(( SELECT CASE status WHEN 4 THEN '#FF4D4D' WHEN 7 THEN '#8DE28D' ELSE '#FFFF66' END AS [td/@bgcolor]
	, td = CONVERT(VARCHAR(100), (case [status]  when 1 then 'Created' when 2 then 'Running' when 3 then 'Canceled' when 4 then 'Failed' when 5 then 'Pending' when 6 then 'Ended Unexpectedly' when 7 then 'Succeeded' when 8 then 'Stopping' when 9 then 'Completed' end))
	, ''
	, td = CONVERT(VARCHAR(255),[execution_id])
	, ''
	, td = CONVERT(VARCHAR(255),[folder_name])
	, ''
	, td = CONVERT(VARCHAR(255),[project_name])
	, ''
	, td = CONVERT(VARCHAR(255),[package_name])
	, ''
	, td = CONVERT(VARCHAR(255),[environment_name])
	, ''
	, td = CONVERT(VARCHAR(255),case [use32bitruntime] when 1 then '32 bit' else '64 bit' end)
	, ''
	, td = CONVERT(VARCHAR(100),convert (varchar(20),[start_time],100))
	, ''
	, td = CONVERT(VARCHAR(500),convert (varchar(20),[end_time],100))
	, ''
	, td = CONVERT(VARCHAR(100),cast( dateadd(ms, datediff(ms, start_time, end_time),0) as time(2) ))
	, ''
	, td = CONVERT(VARCHAR(255),[caller_name])
	, ''
	, td = CONVERT(VARCHAR(255),[server_name])
    FROM [SSISDB].[catalog].[executions]
    where [execution_id] = @execution_id
	
	FOR XML PATH('tr'), TYPE) as NVARCHAR(MAX)) + N'</table>'; 

--count ssis step failures even though its probably only 1 or 0 since its an individual execution
set @ssis_execution_id_error_count = 0
set @ssis_execution_id_error_count = (select count(status) from ssisdb.catalog.executions where execution_id = @execution_id and status <> 7)

--error messages
set @errors_HTML = N'<H4> SSIS Task Errors for Execution ID: ' + convert(varchar(255),@execution_id) + ' </H4>'
	+ N'<table border = "1" bordercolor = "FF3333">'
	+ N'<th bgcolor = "#DB4D4D">Event Name</th>'
	+ N'<th bgcolor = "#DB4D4D">Time</th>'
	+ N'<th bgcolor = "#DB4D4D">Message</th>'
	+ N'<th bgcolor = "#DB4D4D">Package Name</th>'
	+ N'<th bgcolor = "#DB4D4D">Execution Path</th>'
	+ CAST(( SELECT td = CONVERT(VARCHAR(255),[event_name])
	, ''
	, td = CONVERT(VARCHAR(255), convert(varchar(20),[message_time],100))
	, ''
	, td = CONVERT(VARCHAR(MAX),[message])
	, ''
	, td = CONVERT(VARCHAR(255),[package_name])
	, ''
	, td = CONVERT(VARCHAR(MAX),[execution_path])
  FROM [SSISDB].[catalog].[event_messages]
  where operation_id = @execution_id and event_name='OnError'
	
	FOR XML PATH('tr'), TYPE) as NVARCHAR(MAX)) + N'</table>'; 

set @HTML_SSISDB = isnull(@execution_overview_HTML, '') + '<br>' + isnull(@errors_HTML, '')

--count ssis individual onerror events
set @ssis_event_onerror_count = 0
set @ssis_event_onerror_count = (select count(*) from ssisdb.catalog.event_messages where operation_id = @execution_id and event_name='OnError')

--deliver error tally back to parent script
set @ssis_job_fail = 0
set @ssis_job_fail = @ssis_execution_id_error_count + @ssis_event_onerror_count

END


GO

