USE [msdb]
GO

/****** Object:  StoredProcedure [dbo].[usp_ssisdb_execution_overview_errors_for_report_email]    Script Date: 8/14/2014 2:40:09 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE PROCEDURE [dbo].[usp_ssisdb_execution_overview_errors_for_report_email] @execution_id int, @HTML_SSISDB nvarchar(max) output, @ssis_job_fail int output
AS
BEGIN

DECLARE @execution_overview_HTML nvarchar(max), @errors_HTML nvarchar(max), @ssis_execution_id_error_count int, @ssis_event_onerror_count int

--execution overview
set @execution_overview_HTML =  N'<table style="border:0px solid black;border-top:3px solid #ff9900;border-collapse:collapse;width:800px;margin:0px auto">'
	+ N'<caption style="text-align:left;font-size:13px;font-weight:bold;color:#5e6e65;padding: 15px 0px 5px 0px">SSIS Execution ID: ' + convert(varchar(255),@execution_id) + ' Overview </caption>'
	+ N'<tr bgcolor="#AFDACF">'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Status</th>'
	+ N'<th style="padding: 6px 6px 5px 12px;font-size:12px;text-transform:uppercase;text-align:left;color:#5e6e65;border-bottom:1px solid #cdcdcd">SSISDB Package</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;text-align:left;color:#5e6e65;border-bottom:1px solid #cdcdcd">Start</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Duration</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;text-align:left;color:#5e6e65;border-bottom:1px solid #cdcdcd">Server:Env</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Caller</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#5e6e65;border-bottom:1px solid #cdcdcd">Runtime</th>'
	+ N'<tr bgcolor="#f7f7f7">'
	+ CAST(( SELECT CASE [status] WHEN 4 THEN '#FF4D4D' WHEN 7 THEN '#8DE28D' ELSE '#FFFF66' END AS [td/@bgcolor]
	, 'padding:6px 12px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;' as [td/@style]
	, td = CONVERT(VARCHAR(100), (case [status]  when 1 then 'Created' when 2 then 'Running' when 3 then 'Cancel' when 4 then 'Fail' when 5 then 'Pending' when 6 then 'Ended Unexpectedly' when 7 then 'Success' when 8 then 'Stopping' when 9 then 'Completed' end))
	, ''
	, 'padding:6px 12px 5px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(255),[folder_name]) + ' \ ' + CONVERT(VARCHAR(255),[project_name]) + ' \ ' + CONVERT(VARCHAR(255),[package_name])
	, ''
	, 'padding:6px 12px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(100),convert (varchar(20),[start_time],100))
	, ''
	, 'padding:6px 12px 7px 12px;font-size:11px;text-align:center;color:#5e6e65;border-bottom:1px solid #cdcdcd;background-color:#ffffff;' as [td/@style]
	, td = CONVERT(VARCHAR(100),cast( dateadd(ms, datediff(ms, start_time, end_time),0) as time(2) ))
	, ''
	, 'padding:6px 10px 5px 10px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(255),[server_name]) + ':' + CONVERT(VARCHAR(255),[environment_name])
	, ''
	, 'padding:6px 12px 7px 12px;font-size:11px;color:#5e6e65;border-bottom:1px solid #cdcdcd;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(255),(substring([caller_name], charindex('\',[caller_name]) +1, LEN([caller_name]))))
	, ''
	, 'padding:6px 10px 7px 10px;font-size:11px;text-align:center;color:#5e6e65;border-bottom:1px solid #cdcdcd;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(255),case [use32bitruntime] when 1 then '32 bit' else '64 bit' end)
    FROM [SSISDB].[catalog].[executions]
    where [execution_id] = @execution_id
	
	FOR XML PATH('tr'), TYPE) as NVARCHAR(MAX)) + N'</table>'; 

--count ssis step failures even though its probably only 1 or 0 since its an individual execution
set @ssis_execution_id_error_count = 0
set @ssis_execution_id_error_count = (select count(status) from ssisdb.catalog.executions where execution_id = @execution_id and status <> 7)

--error messages
set @errors_HTML = N'<table style="border:0px solid black;border-top:3px solid #b23535;border-left:1px solid #b23535;border-right:1px solid #b23535;border-collapse:collapse;width:800px;margin:0px auto">'
	+ N'<caption style="text-align:left;font-size:13px;font-weight:bold;color:#b23535;padding: 15px 0px 5px 0px">SSIS Task Errors for Execution ID: ' + convert(varchar(255),@execution_id) + '</caption>'
	+ N'<tr bgcolor="#FF4D4D">'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#f7f7f7;border-bottom:1px solid #b23535">Time</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#f7f7f7;border-bottom:1px solid #b23535">Package Name</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#f7f7f7;border-bottom:1px solid #b23535">Execution Path</th>'
	+ N'<th style="padding: 6px 6px 6px 12px;font-size:12px;text-transform:uppercase;color:#f7f7f7;border-bottom:1px solid #b23535">Message</th>'
	+ CAST(( SELECT 'padding:5px 10px 7px 10px;font-size:11px;color:#5e6e65;border-bottom:1px solid #b23535;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(255), convert(varchar(20),[message_time],100))
	, ''
	, 'padding:5px 10px 7px 10px;font-size:11px;text-align:center;color:#5e6e65;border-bottom:1px solid #b23535;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(255),[package_name])
	, ''
	, 'padding:5px 10px 7px 10px;font-size:11px;color:#5e6e65;border-bottom:1px solid #b23535;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(MAX), replace((replace([execution_path],'\',' \ ')),'_','_ '))
	, ''
	, 'padding:5px 10px 7px 10px;font-size:11px;color:#5e6e65;border-bottom:1px solid #b23535;background-color:#ffffff' as [td/@style]
	, td = CONVERT(VARCHAR(MAX), replace((replace((replace([message],'\','\ ')),'_','_ ')),'.','. ')) 
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



